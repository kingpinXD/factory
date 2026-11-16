package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/kingpinXD/factory/internal/notify"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoSlackTransportRecordsLocallyWithoutRunningScripts(t *testing.T) {
	brain := testBrain(t)
	marker := filepath.Join(brain, "called-slack")
	dir := filepath.Join(brain, "scripts", "harness")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"slack-dm.sh", "slack-post.sh"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n: > '"+marker+"'\nexit 98\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("FACTORY_NO_SLACK", "1")
	var out bytes.Buffer
	d, err := depsForBrain(brain, &out)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Notify.DM(context.Background(), "question stored in event log"); err != nil {
		t.Fatal(err)
	}
	ts, err := d.Post.Post(context.Background(), "fixture-channel", "review request suppressed")
	if ts != "" || !errors.Is(err, notify.ErrNotPosted) {
		t.Fatalf("pretended posted: %q %v", ts, err)
	}
	if !strings.Contains(out.String(), "Slack disabled") || !strings.Contains(out.String(), "question stored") {
		t.Fatalf("missing local record:%s", out.String())
	}
	if _, err = os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("Slack script invoked:%v", err)
	}
	t.Setenv("FACTORY_NO_SLACK", "")
	d, err = depsForBrain(brain, &out)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.Notify.(notify.Slack); !ok {
		t.Fatal("default notifier changed")
	}
	if _, ok := d.Post.(notify.Slack); !ok {
		t.Fatal("default poster changed")
	}
}
