package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/store"
)

// RepoWorker does the slow repo work the tick queued: each work item's
// worktree, cloning a missing clone first (P2), and its cleanup once merged.
// It runs as its own process under its own lock, so a hung clone never
// delays a tick, and answers each request with a worktree_ready, done or
// error event in the item's log. It works until nothing is left.
func RepoWorker(ctx context.Context, brain string, g git.Client, timeout time.Duration, out io.Writer) error {
	unlock, err := store.Lock(brain, "repo-worker")
	if err != nil {
		return err
	}
	defer unlock()
	tried := map[string]bool{}
	for {
		jobs, err := repoJobs(brain)
		if err != nil {
			return err
		}
		jobs = slices.DeleteFunc(jobs, func(j repoJob) bool { return tried[j.key()] })
		if len(jobs) == 0 {
			return nil
		}
		for _, j := range jobs {
			tried[j.key()] = true
			if j.req.Request == cleanupRequest {
				// This independent worker can recover a request queued before a crash.
				// Admission remains durably closed before any Git deletion starts.
				if err := fenceCleanup(ctx, brain, j.id); err != nil {
					return err
				}
			}
			res := j.do(ctx, g, timeout)
			if _, err := events.Append(store.EventsPath(j.dir), res); err != nil {
				return err
			}
			fmt.Fprintf(out, "%s: %s: %s %s\n", j.id, j.req.Request, res.Kind, res.Text)
		}
	}
}

// repoJob is one request to the repo worker that has no answer yet.
type repoJob struct {
	id, dir string
	req     events.Event
	evs     []events.Event
	in      WorkInputs
}

func (j repoJob) key() string { return eventRef(j.id, j.req) }

// repoJobs returns every unanswered repo request, by item id.
func repoJobs(brain string) ([]repoJob, error) {
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return nil, err
	}
	var jobs []repoJob
	for _, id := range slices.Sorted(maps.Keys(ix)) {
		entry := ix[id]
		if entry.Kind != blueprint.MachineWork {
			continue
		}
		evs, err := events.Read(store.EventsPath(entry.Dir))
		if err != nil {
			return nil, err
		}
		it := &entity{id: id, entry: entry, evs: evs}
		for _, ev := range evs {
			if ev.Kind != events.KindRequest || ev.Recipient != events.RecipientRepoWorker {
				continue
			}
			if _, answered := repoResult(it, ev); answered {
				continue
			}
			j := repoJob{id: id, dir: entry.Dir, req: ev, evs: evs}
			if err := store.ReadInputs(entry.Dir, &j.in); err != nil {
				return nil, err
			}
			jobs = append(jobs, j)
		}
	}
	return jobs, nil
}

// do runs the job and returns the event that answers it.
func (j repoJob) do(ctx context.Context, g git.Client, timeout time.Duration) events.Event {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res := events.Event{Sender: events.SenderProgram, TriggerRef: seqRef(j.req), Key: repoResultKey(j.req)}
	var err error
	switch j.req.Request {
	case worktreeRequest:
		res.Kind = events.KindWorktreeReady
		res.Text, err = j.worktree(ctx, g)
	case cleanupRequest:
		res.Kind = events.KindDone
		res.Text, err = j.cleanup(ctx, g)
	default:
		err = fmt.Errorf("unknown repo request %q", j.req.Request)
	}
	if err != nil {
		res.Kind, res.Text = events.KindError, fmt.Sprintf("%s: %v", j.req.Request, err)
	}
	return res
}

// worktree makes the item's worktree, only while the item is starting, or
// in pr_open on an adopted PR that skipped the orchestrator, and its
// worktree was never handed over: git.Worktree remakes a worktree that is
// not clean, so it must never run on one an agent works in. An adopted PR's
// worktree is at the PR's head.
func (j repoJob) worktree(ctx context.Context, g git.Client) (string, error) {
	it := &entity{evs: j.evs, work: &j.in}
	t, _ := lastTransition(j.evs)
	adopted := t.To == workPROpen && skipsOrchestrator(it)
	switch {
	case handedOver(it):
		return "", errors.New("the worktree was handed over; it is never made again")
	case t.To != stateStarting && !adopted:
		return "", fmt.Errorf("the item is %s, not starting; its worktree is made only as it starts, or as its adopted PR goes to review", t.To)
	}
	if _, err := os.Stat(filepath.Join(j.in.Clone, ".git")); errors.Is(err, os.ErrNotExist) {
		if err := g.Clone(ctx, j.in.Repo, j.in.Clone); err != nil {
			return "", err
		}
	}
	if adopted {
		return g.WorktreeAtPR(ctx, j.in.Clone, j.in.PR, j.id)
	}
	return g.Worktree(ctx, j.in.Clone, j.in.Base, j.id)
}

// cleanup removes the item's worktree and local branch, keeping either one
// that holds work its merged head does not contain. Once its PR merged, it
// also deletes the factory branch on origin, unless the item adopted the PR:
// that branch is the user's.
func (j repoJob) cleanup(ctx context.Context, g git.Client) (string, error) {
	st, err := store.ReadStatus(j.dir)
	if err != nil {
		return "", err
	}
	merged := st.HeadSHA
	if j.req.Text == cancelledCleanup {
		merged = ""
	}
	kept, err := g.Cleanup(ctx, j.in.Clone, j.id, st.PR, merged)
	if err != nil {
		return "", err
	}
	if j.prMerged() && j.in.PR == 0 {
		remoteKept, err := g.DeleteMergedBranch(ctx, j.in.Clone, j.id, st.HeadSHA)
		if err != nil {
			return "", err
		}
		if remoteKept {
			kept = append(kept, "origin/"+git.Branch(j.id))
		}
	}
	if len(kept) > 0 {
		return "cleanup: kept " + strings.Join(kept, ", ") + ": not in the merged head " + st.HeadSHA, nil
	}
	return cleanupRequest, nil
}

// prMerged reports whether the item's PR merged: the tick logged its move to
// merged before it asked for this cleanup.
func (j repoJob) prMerged() bool {
	return slices.ContainsFunc(j.evs, func(ev events.Event) bool { return ev.Kind == events.KindTransition && ev.To == stateMerged })
}
