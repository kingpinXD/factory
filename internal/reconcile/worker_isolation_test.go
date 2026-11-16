package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

func workerIsolationWorld(t *testing.T) *world {
	t.Helper()
	w := newWorld(t)
	oneSet(w)
	w.tick(false)
	if err := os.MkdirAll(filepath.Join(w.home, "src/r/.git"), 0755); err != nil {
		t.Fatal(err)
	}
	return w
}

func assertWorkerIsolationReady(t *testing.T, w *world) {
	t.Helper()
	ready := w.kinds("e1-w1", events.KindWorktreeReady)
	if len(ready) != 1 || ready[0].Text != git.WorktreePath(filepath.Join(w.home, "src/r"), "e1-w1") {
		t.Fatalf("independent Claude worktree did not finish: %+v\n%s", ready, w.out.String())
	}
}

func TestRepoWorkerIsolatesFailedPassesAndItemCollection(t *testing.T) {
	for _, failure := range []string{"effect ledger", "read owner log", "work inputs", "cleanup fence"} {
		t.Run(failure, func(t *testing.T) {
			w := workerIsolationWorld(t)
			bad := w.epic("e0", "running")
			var input []byte
			var request events.Event
			switch failure {
			case "effect ledger":
				w.append(bad, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: effectRequest, Text: strings.Repeat("1", 64)})
				if err := os.MkdirAll(filepath.Join(w.brain, "factory/effects"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(w.brain, "factory/effects/ledger.json"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "read owner log":
				put(t, store.EventsPath(bad), "{bad event}\n")
			case "work inputs", "cleanup fence":
				w.set("e0", "e0-s1", "working", "e0-w1")
				bad = w.item("e0", "e0-s1", "e0-w1", stateStarting)
				kind := worktreeRequest
				if failure == "cleanup fence" {
					kind = cleanupRequest
				}
				request = w.append(bad, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: kind})
				if failure == "work inputs" {
					var err error
					input, err = os.ReadFile(store.InputsPath(bad))
					if err != nil {
						t.Fatal(err)
					}
					put(t, store.InputsPath(bad), "[broken yaml")
				} else {
					put(t, filepath.Join(w.brain, "factory/helpers/ledger.json"), "{}")
				}
			}
			original, err := os.ReadFile(store.EventsPath(bad))
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, PublicationWorker{}); err != nil {
					t.Fatalf("one held request stopped the worker: %v\n%s", err, w.out.String())
				}
				assertWorkerIsolationReady(t, w)
				after, err := os.ReadFile(store.EventsPath(bad))
				if err != nil || string(after) != string(original) {
					t.Fatalf("held request was answered or changed: %v\n%s", err, after)
				}
			}
			if !strings.Contains(w.out.String(), "held:") {
				t.Fatal("held failure was not reported", w.out.String())
			}
			if failure == "work inputs" {
				if err := os.WriteFile(store.InputsPath(bad), input, 0644); err != nil {
					t.Fatal(err)
				}
				if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, PublicationWorker{}); err != nil {
					t.Fatal(err)
				}
				logged := w.log("e0-w1")
				answer, found := repoResult(&entity{evs: logged}, request)
				if !found || answer.Kind != events.KindWorktreeReady {
					t.Fatalf("repaired request was not retried: %+v %v", answer, found)
				}
				assertWorkerIsolationReady(t, w)
			}
		})
	}
}

func TestProgramReadWorkerIsolatesUnreadableOwnerLog(t *testing.T) {
	w := workerIsolationWorld(t)
	bad := w.epic("e0", "running")
	put(t, store.EventsPath(bad), "{bad event}\n")
	if err := os.MkdirAll(filepath.Join(w.brain, "factory/codex/program-reads/raw-temp"), 0700); err != nil {
		t.Fatal(err)
	}
	brain, err := filepath.EvalSymlinks(w.brain)
	if err != nil {
		t.Fatal(err)
	}
	if err := runProgramReadJobsUsing(context.Background(), brain, &w.out, b2Qualification); err != nil {
		t.Fatalf("one owner stopped the read pass: %v", err)
	}
	if !strings.Contains(w.out.String(), "e0: programme read queue held:") || !strings.Contains(w.out.String(), "raw-temp: programme read evidence held:") {
		t.Fatal("read pass did not reach the next independent request", w.out.String())
	}
}

func TestEffectWorkerIsolatesUnreadableOwnerLog(t *testing.T) {
	w := workerIsolationWorld(t)
	bad := w.epic("e0", "running")
	put(t, store.EventsPath(bad), "{bad event}\n")
	w.append(w.dir("e1"), events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: effectRequest, Text: strings.Repeat("1", 64)})
	for _, dir := range []string{filepath.Join(w.brain, "factory"), filepath.Join(w.brain, "factory/.lock"), filepath.Join(w.brain, "factory/effects")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(w.brain, "factory/effects/ledger.json"), []byte(`{"schema":2,"revision":0,"actions":{},"attempts":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := runEffectJobs(context.Background(), w.brain, &w.out); err != nil {
		t.Fatalf("one owner stopped the effects pass: %v", err)
	}
	if !strings.Contains(w.out.String(), "e0: effect queue held:") || !strings.Contains(w.out.String(), "e1: effect queue held:") {
		t.Fatal("effects pass did not reach the next owner", w.out.String())
	}
}

func TestRepoWorkerIsolatesFailedResultAppend(t *testing.T) {
	w := workerIsolationWorld(t)
	w.epic("e0", "running")
	w.set("e0", "e0-s1", "working", "e0-w1")
	bad := w.item("e0", "e0-s1", "e0-w1", stateStarting)
	request := w.append(bad, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	original := store.EventsPath(bad)
	backup := original + ".saved"
	interrupted := false
	runner := &proc.Fake{Respond: func(command proc.Cmd) ([]byte, error) {
		if !interrupted && command.Name == "git" && strings.Contains(strings.Join(command.Args, " "), "factory/e0-w1") {
			interrupted = true
			if err := os.Rename(original, backup); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(original, 0700); err != nil {
				t.Fatal(err)
			}
		}
		return w.fake.Run(context.Background(), command)
	}}
	if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: runner}, time.Second, &w.out, PublicationWorker{}); err != nil {
		t.Fatalf("one append failure stopped the next job: %v", err)
	}
	if !interrupted {
		t.Fatal("fixture did not interrupt the result append")
	}
	assertWorkerIsolationReady(t, w)
	if err := os.Remove(original); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(backup, original); err != nil {
		t.Fatal(err)
	}
	if _, found := repoResult(&entity{evs: w.log("e0-w1")}, request); found {
		t.Fatal("failed append answered the request")
	}
	if err := RepoWorker(context.Background(), w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, PublicationWorker{}); err != nil {
		t.Fatal(err)
	}
	if answer, found := repoResult(&entity{evs: w.log("e0-w1")}, request); !found || answer.Kind != events.KindWorktreeReady {
		t.Fatalf("append repair was not retried: %+v %v", answer, found)
	}
}

func TestRepoWorkerStillStopsOnGlobalIndexFailureAndCancellation(t *testing.T) {
	for _, failure := range []string{"index", "cancelled"} {
		t.Run(failure, func(t *testing.T) {
			w := workerIsolationWorld(t)
			ctx := context.Background()
			if failure == "index" {
				put(t, store.IndexPath(w.brain), "{bad index}")
			} else {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := RepoWorker(ctx, w.brain, git.Client{Runner: w.fake}, time.Second, &w.out, PublicationWorker{})
			if err == nil || failure == "cancelled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("unsafe worker failure was ignored: %v", err)
			}
			if len(w.called("git ")) != 0 {
				t.Fatal("unsafe worker started repo work")
			}
		})
	}
}

func TestRepoWorkerCancellationStopsRemainingJobs(t *testing.T) {
	w := workerIsolationWorld(t)
	w.epic("e0", "running")
	w.set("e0", "e0-s1", "working", "e0-w1")
	bad := w.item("e0", "e0-s1", "e0-w1", stateStarting)
	w.append(bad, events.Event{Kind: events.KindRequest, Sender: events.SenderProgram, Recipient: events.RecipientRepoWorker, Request: worktreeRequest})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled := false
	runner := &proc.Fake{Respond: func(command proc.Cmd) ([]byte, error) {
		if command.Name == "git" && strings.Contains(strings.Join(command.Args, " "), "factory/e0-w1") {
			cancelled = true
			cancel()
		}
		return w.fake.Run(context.Background(), command)
	}}
	err := RepoWorker(ctx, w.brain, git.Client{Runner: runner}, time.Second, &w.out, PublicationWorker{})
	if !cancelled || !errors.Is(err, context.Canceled) {
		t.Fatalf("fixture did not cancel mid-job: cancelled=%v err=%v", cancelled, err)
	}
	if ready := w.kinds("e1-w1", events.KindWorktreeReady); len(ready) != 0 {
		t.Fatalf("cancelled worker answered another job: %+v", ready)
	}
	for _, command := range runner.Calls() {
		if strings.Contains(strings.Join(command.Args, " "), "factory/e1-w1") {
			t.Fatal("cancelled worker started another item's Git operation")
		}
	}
}
