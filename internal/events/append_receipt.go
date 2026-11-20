package events

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
)

type EventReceipt struct {
	Event       Event  `json:"event"`
	EventSHA256 string `json:"event_sha256"`
	CallerKey   string `json:"caller_key"`
}
type EventReceiptRef = AckReceiptRef

func controlEventReceiptName(key string) string {
	return fmt.Sprintf("%x.json", sha256.Sum256([]byte(key)))
}

func validateControlledEvent(path string, event Event) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || event.Key == "" || event.Sender == "" || event.Seq != 0 || !event.At.IsZero() || !slices.Contains(Kinds, event.Kind) || event.Kind == KindAck {
		return fmt.Errorf("invalid controlled event binding")
	}
	return nil
}
func eventEqual(first, second Event) bool { return reflect.DeepEqual(first, second) }
func AppendWithReceipt(path string, event Event, callerKey string) (EventReceiptRef, error) {
	if err := validateControlledEvent(path, event); err != nil {
		return EventReceiptRef{}, err
	}
	log, err := LockControlEventLog(context.Background(), path)
	if err != nil {
		return EventReceiptRef{}, err
	}
	defer log.Close()
	return log.AppendWithReceipt(event, callerKey)
}
func appendEventReceiptLocked(path string, f *os.File, event Event, callerKey string) (EventReceiptRef, error) {
	actual, err := appendLocked(path, f, event)
	if err != nil {
		return EventReceiptRef{}, err
	}
	compare := actual
	compare.Seq = 0
	compare.At = event.At
	if !reflect.DeepEqual(compare, event) {
		return EventReceiptRef{}, fmt.Errorf("controlled event key conflicts")
	}
	if err := f.Sync(); err != nil {
		return EventReceiptRef{}, err
	}
	if err := syncAckPath(filepath.Dir(path)); err != nil {
		return EventReceiptRef{}, err
	}
	return publishControlledEventReceipt(path, actual, callerKey)
}
func publishControlledEventReceipt(path string, actual Event, callerKey string) (EventReceiptRef, error) {
	receipt := EventReceipt{Event: actual, EventSHA256: eventDigest(actual), CallerKey: callerKey}
	data, err := json.Marshal(receipt)
	if err != nil {
		return EventReceiptRef{}, err
	}
	return publishEventReceiptArtifact(path, "control", controlEventReceiptName(actual.Key), data)
}

func RecoverEventReceipt(path string, event Event, callerKey string) (EventReceiptRef, error) {
	return AppendWithReceipt(path, event, callerKey)
}

func ReadEventReceipt(ref EventReceiptRef) (EventReceipt, error) {
	var receipt EventReceipt
	data, err := readEventReceiptArtifact(ref, 8<<20)
	if err != nil {
		return receipt, err
	}
	if err := json.Unmarshal(data, &receipt); err != nil {
		return receipt, err
	}
	canonical, _ := json.Marshal(receipt)
	if !bytes.Equal(canonical, data) || receipt.Event.Seq < 1 || receipt.Event.At.IsZero() || receipt.Event.Key == "" || receipt.Event.Sender == "" || !slices.Contains(Kinds, receipt.Event.Kind) || receipt.Event.Kind == KindAck || receipt.EventSHA256 != eventDigest(receipt.Event) || filepath.Base(ref.Path) != controlEventReceiptName(receipt.Event.Key) || filepath.Base(filepath.Dir(ref.Path)) != "control" || filepath.Base(filepath.Dir(filepath.Dir(ref.Path))) != ".event-receipts" {
		return receipt, fmt.Errorf("invalid controlled event receipt")
	}
	return receipt, nil
}

// The actual log append lock serializes publication of each domain receipt.
func publishEventReceiptArtifact(logPath, domain, name string, data []byte) (AckReceiptRef, error) {
	if (domain != "ack" && domain != "control") || filepath.Base(name) != name || !strings.HasSuffix(name, ".json") || len(data) > 8<<20 {
		return AckReceiptRef{}, fmt.Errorf("invalid event receipt namespace")
	}
	dir := filepath.Dir(logPath)
	for _, part := range []string{".event-receipts", domain} {
		parent := dir
		dir = filepath.Join(dir, part)
		if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
			return AckReceiptRef{}, err
		}
		st, err := os.Lstat(dir)
		if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
			return AckReceiptRef{}, fmt.Errorf("event receipt directory is not private")
		}
		if err := syncAckPath(parent); err != nil {
			return AckReceiptRef{}, err
		}
	}
	ref := AckReceiptRef{Path: filepath.Join(dir, name), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	if _, err := os.Lstat(ref.Path); err == nil {
		current, err := readEventReceiptArtifact(ref, 8<<20)
		if err != nil || !bytes.Equal(current, data) {
			return AckReceiptRef{}, fmt.Errorf("event receipt conflicts")
		}
		if err := syncAckPath(ref.Path); err != nil {
			return AckReceiptRef{}, err
		}
		return ref, syncAckPath(dir)
	} else if !os.IsNotExist(err) {
		return AckReceiptRef{}, err
	}
	f, err := os.CreateTemp(dir, ".event-receipt-*")
	if err != nil {
		return AckReceiptRef{}, err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(f.Name(), ref.Path)
	}
	if err == nil {
		err = syncAckPath(dir)
	}
	return ref, err
}

func readEventReceiptArtifact(ref AckReceiptRef, limit int64) ([]byte, error) {
	sum, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(sum) != sha256.Size || !filepath.IsAbs(ref.Path) || filepath.Clean(ref.Path) != ref.Path {
		return nil, fmt.Errorf("invalid event receipt reference")
	}
	if real, err := filepath.EvalSymlinks(ref.Path); err != nil || real != ref.Path {
		return nil, fmt.Errorf("event receipt path is aliased")
	}
	f, err := os.OpenFile(ref.Path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	id, ok := st.Sys().(*syscall.Stat_t)
	if !ok || id.Nlink != 1 || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || st.Size() > limit {
		return nil, fmt.Errorf("invalid event receipt file")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(data)) > limit || fmt.Sprintf("%x", sha256.Sum256(data)) != ref.SHA256 {
		return nil, fmt.Errorf("event receipt changed")
	}
	return data, nil
}
