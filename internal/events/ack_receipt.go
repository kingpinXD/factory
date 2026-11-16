package events

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

type AckReceipt struct {
	Message       Event  `json:"message"`
	Ack           Event  `json:"ack"`
	MessageSHA256 string `json:"message_sha256"`
	AckSHA256     string `json:"ack_sha256"`
}

type AckReceiptRef struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func eventDigest(e Event) string {
	data, _ := json.Marshal(e)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func AckWithReceipt(path string, seq int, sender string) (AckReceiptRef, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || seq < 1 || sender == "" {
		return AckReceiptRef{}, fmt.Errorf("invalid acknowledgement binding")
	}
	log, err := lockControlEventLog(context.Background(), path, "acknowledgement")
	if err != nil {
		return AckReceiptRef{}, err
	}
	defer log.Close()
	return log.AckWithReceipt(seq, sender)
}
func ackWithReceiptLocked(path string, f *os.File, seq int, sender string) (AckReceiptRef, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return AckReceiptRef{}, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return AckReceiptRef{}, err
	}
	whole, _ := cutTorn(data)
	logged, err := decode(path, whole)
	if err != nil {
		return AckReceiptRef{}, err
	}
	for i, event := range logged {
		if event.Seq != i+1 {
			return AckReceiptRef{}, fmt.Errorf("acknowledgement history is incomplete")
		}
	}
	i := slices.IndexFunc(logged, func(e Event) bool { return e.Seq == seq })
	if i < 0 || !IsMessage(logged[i]) {
		return AckReceiptRef{}, fmt.Errorf("seq %d is not a message in %s", seq, path)
	}
	ack, err := appendLocked(path, f, Event{Kind: KindAck, Ack: seq, Sender: sender, Key: fmt.Sprintf("ack:%d", seq)})
	if err != nil {
		return AckReceiptRef{}, err
	}
	if ack.Kind != KindAck || ack.Ack != seq || ack.Key != fmt.Sprintf("ack:%d", seq) || ack.Seq <= seq {
		return AckReceiptRef{}, fmt.Errorf("acknowledgement event conflicts")
	}
	if err := f.Sync(); err != nil {
		return AckReceiptRef{}, err
	}
	if err := syncAckPath(filepath.Dir(path)); err != nil {
		return AckReceiptRef{}, err
	}
	receipt := AckReceipt{Message: logged[i], Ack: ack, MessageSHA256: eventDigest(logged[i]), AckSHA256: eventDigest(ack)}
	return publishAckReceipt(path, receipt)
}

func RecoverAckReceipt(path string, seq int, sender string) (AckReceiptRef, error) {
	return AckWithReceipt(path, seq, sender)
}

func publishAckReceipt(logPath string, receipt AckReceipt) (AckReceiptRef, error) {
	data, err := json.Marshal(receipt)
	if err != nil {
		return AckReceiptRef{}, err
	}
	return publishEventReceiptArtifact(logPath, "ack", strconv.Itoa(receipt.Message.Seq)+".json", data)
}

func ReadAckReceipt(ref AckReceiptRef) (AckReceipt, error) {
	var receipt AckReceipt
	data, err := readEventReceiptArtifact(ref, 4<<20)
	if err != nil {
		return receipt, err
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		return receipt, err
	}
	canonical, _ := json.Marshal(receipt)
	if !bytes.Equal(data, canonical) || !IsMessage(receipt.Message) || receipt.Message.Seq < 1 || receipt.Ack.Kind != KindAck || receipt.Ack.Ack != receipt.Message.Seq || receipt.Ack.Seq <= receipt.Message.Seq || receipt.Ack.Key != fmt.Sprintf("ack:%d", receipt.Message.Seq) || receipt.MessageSHA256 != eventDigest(receipt.Message) || receipt.AckSHA256 != eventDigest(receipt.Ack) {
		return receipt, fmt.Errorf("invalid acknowledgement receipt binding")
	}
	if filepath.Base(ref.Path) != strconv.Itoa(receipt.Message.Seq)+".json" || filepath.Base(filepath.Dir(ref.Path)) != "ack" || filepath.Base(filepath.Dir(filepath.Dir(ref.Path))) != ".event-receipts" {
		return receipt, fmt.Errorf("acknowledgement receipt is outside its domain")
	}
	return receipt, nil
}

func syncAckPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
