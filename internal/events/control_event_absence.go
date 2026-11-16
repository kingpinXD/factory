package events

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type ControlEventLog struct {
	file    *os.File
	path    string
	context context.Context
}
type ControlEventPrefix struct {
	Bytes  int64
	SHA256 string
}

var ErrControlEventPresent = errors.New("controlled event already exists")

func LockControlEventLog(ctx context.Context, path string) (*ControlEventLog, error) {
	return lockControlEventLog(ctx, path, "controlled event")
}
func lockControlEventLog(ctx context.Context, path, label string) (*ControlEventLog, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("invalid %s log", label)
	}
	if real, err := filepath.EvalSymlinks(path); err != nil || real != path {
		return nil, fmt.Errorf("%s log is not canonical", label)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*ControlEventLog, error) { file.Close(); return nil, err }
	stat, err := file.Stat()
	if err != nil {
		return fail(err)
	}
	identity, ok := stat.Sys().(*syscall.Stat_t)
	if !ok || identity.Nlink != 1 || !stat.Mode().IsRegular() {
		return fail(fmt.Errorf("%s log is aliased", label))
	}
	for {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return &ControlEventLog{file: file, path: path, context: ctx}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return fail(err)
		}
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func (log *ControlEventLog) Close() error {
	if log == nil || log.file == nil {
		return nil
	}
	file := log.file
	log.file = nil
	return file.Close()
}
func (log *ControlEventLog) valid() error {
	if log == nil || log.file == nil {
		return fmt.Errorf("event log lock is closed")
	}
	return log.context.Err()
}
func (log *ControlEventLog) AppendWithReceipt(event Event, callerKey string) (EventReceiptRef, error) {
	if err := log.valid(); err != nil {
		return EventReceiptRef{}, err
	}
	if err := validateControlledEvent(log.path, event); err != nil {
		return EventReceiptRef{}, err
	}
	return appendEventReceiptLocked(log.path, log.file, event, callerKey)
}
func (log *ControlEventLog) AckWithReceipt(sequence int, sender string) (AckReceiptRef, error) {
	if err := log.valid(); err != nil {
		return AckReceiptRef{}, err
	}
	if sequence < 1 || sender == "" {
		return AckReceiptRef{}, fmt.Errorf("invalid acknowledgement binding")
	}
	return ackWithReceiptLocked(log.path, log.file, sequence, sender)
}

func (log *ControlEventLog) scan(visit func(Event) error) (ControlEventPrefix, error) {
	if err := log.valid(); err != nil {
		return ControlEventPrefix{}, err
	}
	if _, err := log.file.Seek(0, io.SeekStart); err != nil {
		return ControlEventPrefix{}, err
	}
	return scanControlEventPrefix(log.context, log.path, log.file, visit)
}
func scanControlEventPrefix(ctx context.Context, path string, reader io.Reader, visit func(Event) error) (ControlEventPrefix, error) {
	return scanControlEventRange(ctx, path, reader, 0, false, visit)
}

func scanControlEventRange(ctx context.Context, path string, reader io.Reader, initialSequence int, allowEmpty bool, visit func(Event) error) (ControlEventPrefix, error) {
	hash := sha256.New()
	counter := &eventByteCounter{}
	scanner := bufio.NewScanner(io.TeeReader(reader, io.MultiWriter(hash, counter)))
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		for i, value := range data {
			if value == '\n' {
				return i + 1, data[:i], nil
			}
		}
		if atEOF && len(data) > 0 {
			return 0, nil, fmt.Errorf("event log has a partial final line")
		}
		return 0, nil, nil
	})
	sequence := initialSequence
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return ControlEventPrefix{}, err
		}
		decoded, err := decode(path, append(append([]byte(nil), scanner.Bytes()...), '\n'))
		if err != nil {
			return ControlEventPrefix{}, fmt.Errorf("event prefix is invalid: %w", err)
		}
		if len(decoded) != 1 {
			return ControlEventPrefix{}, fmt.Errorf("event prefix line does not contain one event")
		}
		sequence++
		if decoded[0].Seq != sequence {
			return ControlEventPrefix{}, fmt.Errorf("event prefix sequence is incomplete")
		}
		if err := visit(decoded[0]); err != nil {
			return ControlEventPrefix{}, err
		}
	}
	if err := scanner.Err(); err != nil {
		return ControlEventPrefix{}, err
	}
	if sequence == initialSequence && !allowEmpty {
		return ControlEventPrefix{}, fmt.Errorf("event prefix is empty")
	}
	return ControlEventPrefix{Bytes: counter.bytes, SHA256: fmt.Sprintf("%x", hash.Sum(nil))}, nil
}

func (log *ControlEventLog) snapshotSize() (int64, error) {
	if err := log.valid(); err != nil {
		return 0, err
	}
	real, err := filepath.EvalSymlinks(log.path)
	if err != nil || real != log.path {
		return 0, fmt.Errorf("event log is not canonical")
	}
	current, err := os.Lstat(log.path)
	if err != nil {
		return 0, err
	}
	opened, err := log.file.Stat()
	if err != nil {
		return 0, err
	}
	identity, ok := opened.Sys().(*syscall.Stat_t)
	if !ok || identity.Nlink != 1 || !current.Mode().IsRegular() || !os.SameFile(current, opened) {
		return 0, fmt.Errorf("event log identity changed")
	}
	return opened.Size(), nil
}

// Snapshot reads one captured finite prefix. Release the log lock before any
// domain reader that opens this same event file.
func (log *ControlEventLog) Snapshot() ([]Event, ControlEventPrefix, error) {
	size, err := log.snapshotSize()
	if err != nil {
		return nil, ControlEventPrefix{}, err
	}
	if _, err := log.file.Seek(0, io.SeekStart); err != nil {
		return nil, ControlEventPrefix{}, err
	}
	var found []Event
	prefix, err := scanControlEventRange(log.context, log.path, io.LimitReader(log.file, size), 0, false, func(event Event) error {
		found = append(found, event)
		return nil
	})
	if err != nil {
		return nil, ControlEventPrefix{}, err
	}
	return found, prefix, nil
}

// SuffixAfter verifies the captured prefix before returning a finite suffix.
func (log *ControlEventLog) SuffixAfter(expected ControlEventPrefix) ([]Event, error) {
	size, err := log.snapshotSize()
	if err != nil {
		return nil, err
	}
	if expected.Bytes < 1 || expected.Bytes > size {
		return nil, fmt.Errorf("event snapshot prefix changed")
	}
	if _, err := log.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	last := 0
	actual, err := scanControlEventRange(log.context, log.path, io.LimitReader(log.file, expected.Bytes), 0, false, func(event Event) error {
		last = event.Seq
		return nil
	})
	if err != nil {
		return nil, err
	}
	if actual != expected {
		return nil, fmt.Errorf("event snapshot prefix changed")
	}
	found := []Event{}
	_, err = scanControlEventRange(log.context, log.path, io.LimitReader(log.file, size-expected.Bytes), last, true, func(event Event) error {
		found = append(found, event)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

type eventByteCounter struct{ bytes int64 }

func (counter *eventByteCounter) Write(bytes []byte) (int, error) {
	counter.bytes += int64(len(bytes))
	return len(bytes), nil
}

func (log *ControlEventLog) LookupEventReceipt(event Event, callerKey string) (EventReceiptRef, error) {
	var found *Event
	_, err := log.scan(func(actual Event) error {
		if actual.Key != event.Key {
			return nil
		}
		if found != nil {
			return fmt.Errorf("controlled event key is duplicated")
		}
		copy := actual
		copy.Seq = 0
		copy.At = event.At
		if !eventEqual(copy, event) {
			return fmt.Errorf("controlled event key conflicts")
		}
		value := actual
		found = &value
		return nil
	})
	if err != nil {
		return EventReceiptRef{}, err
	}
	if found == nil {
		return EventReceiptRef{}, os.ErrNotExist
	}
	if err := log.file.Sync(); err != nil {
		return EventReceiptRef{}, err
	}
	if err := syncAckPath(filepath.Dir(log.path)); err != nil {
		return EventReceiptRef{}, err
	}
	return publishControlledEventReceipt(log.path, *found, callerKey)
}
func (log *ControlEventLog) LookupAckReceipt(sequence int) (AckReceiptRef, error) {
	var message, ack *Event
	_, err := log.scan(func(actual Event) error {
		if actual.Seq == sequence {
			value := actual
			message = &value
		}
		if actual.Key != "ack:"+strconv.Itoa(sequence) {
			return nil
		}
		if ack != nil || actual.Kind != KindAck || actual.Ack != sequence {
			return fmt.Errorf("acknowledgement key conflicts")
		}
		value := actual
		ack = &value
		return nil
	})
	if err != nil {
		return AckReceiptRef{}, err
	}
	if message == nil || !IsMessage(*message) {
		return AckReceiptRef{}, fmt.Errorf("seq %d is not a message in %s", sequence, log.path)
	}
	if ack == nil {
		return AckReceiptRef{}, os.ErrNotExist
	}
	if ack.Seq <= message.Seq {
		return AckReceiptRef{}, fmt.Errorf("acknowledgement event conflicts")
	}
	if err := log.file.Sync(); err != nil {
		return AckReceiptRef{}, err
	}
	if err := syncAckPath(filepath.Dir(log.path)); err != nil {
		return AckReceiptRef{}, err
	}
	return publishAckReceipt(log.path, AckReceipt{Message: *message, Ack: *ack, MessageSHA256: eventDigest(*message), AckSHA256: eventDigest(*ack)})
}
func (log *ControlEventLog) ObserveAbsence(key string, ack int) (ControlEventPrefix, error) {
	if (key == "") == (ack < 1) {
		return ControlEventPrefix{}, fmt.Errorf("invalid event absence binding")
	}
	return log.scan(func(event Event) error {
		if key != "" && event.Key == key || ack > 0 && (event.Key == "ack:"+strconv.Itoa(ack) || event.Kind == KindAck && event.Ack == ack) {
			return ErrControlEventPresent
		}
		return nil
	})
}
func (log *ControlEventLog) Message(sequence int) (Event, error) {
	var found *Event
	_, err := log.scan(func(event Event) error {
		if event.Seq == sequence {
			copy := event
			found = &copy
		}
		return nil
	})
	if err != nil {
		return Event{}, err
	}
	if found == nil || !IsMessage(*found) {
		return Event{}, fmt.Errorf("seq %d is not a message in %s", sequence, log.path)
	}
	return *found, nil
}
func ReadControlEventPrefixLocal(ctx context.Context, path string, prefix ControlEventPrefix, key string, ack int) error {
	if (key == "") == (ack < 1) {
		return fmt.Errorf("invalid event prefix binding")
	}
	return readControlEventPrefixLocal(ctx, path, prefix, func(event Event) error {
		if key != "" && event.Key == key || ack > 0 && (event.Key == "ack:"+strconv.Itoa(ack) || event.Kind == KindAck && event.Ack == ack) {
			return ErrControlEventPresent
		}
		return nil
	})
}
func ReadControlEventPrefixMessageLocal(ctx context.Context, path string, prefix ControlEventPrefix, sequence int) (Event, error) {
	var found *Event
	err := readControlEventPrefixLocal(ctx, path, prefix, func(event Event) error {
		if event.Seq == sequence {
			copy := event
			found = &copy
		}
		return nil
	})
	if err != nil {
		return Event{}, err
	}
	if found == nil || !IsMessage(*found) {
		return Event{}, fmt.Errorf("seq %d is not a message in %s", sequence, path)
	}
	return *found, nil
}
func readControlEventPrefixLocal(ctx context.Context, path string, prefix ControlEventPrefix, visit func(Event) error) error {
	if prefix.Bytes < 1 || len(prefix.SHA256) != sha256.Size*2 || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("invalid event prefix binding")
	}
	if real, err := filepath.EvalSymlinks(path); err != nil || real != path {
		return fmt.Errorf("event prefix path is aliased")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	identity, ok := stat.Sys().(*syscall.Stat_t)
	if !ok || identity.Nlink != 1 || !stat.Mode().IsRegular() || stat.Size() < prefix.Bytes {
		return fmt.Errorf("event prefix file is invalid")
	}
	actual, err := scanControlEventPrefix(ctx, path, io.NewSectionReader(file, 0, prefix.Bytes), visit)
	if err != nil {
		return err
	}
	if actual != prefix {
		return fmt.Errorf("event prefix changed")
	}
	return nil
}

var agentEventKinds = []string{KindStart, KindEnd, KindError, KindIntent, KindDone, KindIssueStale, KindBlockedBy, KindInstruction, KindMessage, KindCheckpoint}

func AgentEventKinds() []string { return append([]string(nil), agentEventKinds...) }
func CoordinatorEventKinds() []string {
	var kinds []string
	for _, kind := range agentEventKinds {
		if kind != KindIssueStale {
			kinds = append(kinds, kind)
		}
	}
	return kinds
}
func ValidateAgentEvent(kind, file, text, to string) error {
	switch {
	case !slices.Contains(Kinds, kind):
		return fmt.Errorf("unknown event kind: %s", kind)
	case !slices.Contains(agentEventKinds, kind):
		return fmt.Errorf("%s events are written by the program, not by factory event", kind)
	case (kind == KindEnd || kind == KindCheckpoint) && file == "":
		return fmt.Errorf("%s needs --file <path>", kind)
	case (kind == KindMessage || kind == KindInstruction || kind == KindBlockedBy) && text == "":
		return fmt.Errorf("%s needs --text", kind)
	case strings.ContainsAny(text, "\r\n"):
		return errors.New("--text must be one line")
	case to != "" && kind != KindMessage:
		return errors.New("--to applies only to message events")
	}
	return nil
}
