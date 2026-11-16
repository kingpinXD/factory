package codex

import (
	"context"
	"github.com/kingpinXD/factory/internal/session"
	"os"
	"path/filepath"
	"testing"
)

func TestInventoryMalformedRecordKeepsHealthySibling(t *testing.T) {
	for _, kind := range []string{"activity", "process", "binding", "corrupt", "foreign_directory"} {
		t.Run(kind, func(t *testing.T) {
			r := New(t.TempDir(), "unused")
			healthy := fixtureRecord(t, r)
			healthy.State = "stopped"
			if err := r.save(&healthy); err != nil {
				t.Fatal(err)
			}
			bad := healthy
			bad.Spec.Name = "factory:bad:planner"
			bad.Spec.Task = "bad"
			bad.ID = hash([]byte(bad.Spec.Name))
			switch kind {
			case "activity":
				bad.State = "invalid"
			case "process":
				bad.Host = ProcessIdentity{PID: 3}
			case "binding":
				bad.Spec.Name = "factory:claimed:planner"
			}
			if err := r.save(&bad); err != nil {
				t.Fatal(err)
			}
			dir, _ := r.directory(bad.ID)
			if kind == "corrupt" {
				if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "foreign_directory" {
				if err := os.Mkdir(filepath.Join(filepath.Dir(dir), "unexpected"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			got, err := r.List(context.Background())
			if err != nil || !got.Complete || got.Lookup(healthy.Spec.Name).Presence != session.Present {
				t.Fatalf("bad record hid healthy sibling: %+v %v", got, err)
			}
			if kind != "foreign_directory" && got.Lookup("factory:bad:planner").Presence != session.Quarantined {
				t.Fatalf("bad fixed slot no longer reserved: %+v", got)
			}
			if kind == "binding" && got.Lookup(bad.Spec.Name).Presence != session.Quarantined {
				t.Fatal("claimed name escaped reservation")
			}
			if got.Lookup("factory:unrelated:planner").Presence != session.Absent {
				t.Fatal("bad record prevented independent admission")
			}
		})
	}
}
