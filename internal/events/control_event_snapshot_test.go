package events

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestControlEventSnapshotAndFiniteSuffix(t *testing.T) {
	name := controlledEventLog(t)
	first, err := Append(name, Event{Kind: KindStart, Sender: SenderProgram})
	if err != nil {
		t.Fatal(err)
	}
	log, err := LockControlEventLog(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, prefix, err := log.Snapshot()
	if err != nil || !reflect.DeepEqual(snapshot, []Event{first}) {
		t.Fatalf("actual snapshot: %+v %v", snapshot, err)
	}
	if suffix, err := log.SuffixAfter(prefix); err != nil || len(suffix) != 0 {
		t.Fatalf("empty suffix: %+v %v", suffix, err)
	}
	log.Close()
	next, err := Append(name, Event{Kind: KindDone, Sender: SenderProgram})
	if err != nil {
		t.Fatal(err)
	}
	log, err = LockControlEventLog(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	suffix, err := log.SuffixAfter(prefix)
	if err != nil || !reflect.DeepEqual(suffix, []Event{next}) {
		t.Fatalf("actual contiguous suffix: %+v %v", suffix, err)
	}
	prefix.SHA256 = strings.Repeat("0", 64)
	if _, err := log.SuffixAfter(prefix); err == nil || !strings.Contains(err.Error(), "prefix changed") {
		t.Fatalf("changed prefix accepted: %v", err)
	}
}

func TestControlEventSnapshotRejectsChangedIdentityAndIncompleteLog(t *testing.T) {
	for _, mode := range []string{"replacement", "hardlink", "symlink", "torn", "sequence"} {
		t.Run(mode, func(t *testing.T) {
			name := controlledEventLog(t)
			if _, err := Append(name, Event{Kind: KindStart, Sender: SenderProgram}); err != nil {
				t.Fatal(err)
			}
			log, err := LockControlEventLog(context.Background(), name)
			if err != nil {
				t.Fatal(err)
			}
			defer log.Close()
			_, prefix, err := log.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "replacement", "symlink":
				if err := os.Rename(name, name+".old"); err != nil {
					t.Fatal(err)
				}
				if mode == "symlink" {
					err = os.Symlink(name+".old", name)
				} else {
					err = os.WriteFile(name, []byte("{}\n"), 0600)
				}
			case "hardlink":
				err = os.Link(name, name+".alias")
			case "torn", "sequence":
				var f *os.File
				f, err = os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0)
				if err == nil {
					data := "{"
					if mode == "sequence" {
						data = "{\"seq\":99,\"kind\":\"done\"}\n"
					}
					_, err = f.WriteString(data)
					if closeErr := f.Close(); err == nil {
						err = closeErr
					}
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := log.Snapshot(); err == nil {
				t.Fatal("changed/partial log became a snapshot")
			}
			if _, err := log.SuffixAfter(prefix); err == nil {
				t.Fatal("changed/partial log became a suffix")
			}
		})
	}
}

func TestControlEventSnapshotCancellation(t *testing.T) {
	name := controlledEventLog(t)
	if _, err := Append(name, Event{Kind: KindStart, Sender: SenderProgram}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	log, err := LockControlEventLog(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	_, prefix, err := log.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, _, err := log.Snapshot(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if _, err := log.SuffixAfter(prefix); !errors.Is(err, context.Canceled) {
		t.Fatalf("suffix cancellation lost: %v", err)
	}
}
