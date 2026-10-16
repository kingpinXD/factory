package helper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/store"
)

func digestBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func validateOutput(db *ledger, r *jobRecord) error {
	q := r.Rounds[r.Job.Round]
	if q.Output == "" {
		return nil
	}
	if !strings.HasPrefix(db.Reservations[q.Output], r.Job.ID+":") {
		return fmt.Errorf("output reservation missing")
	}
	if _, err := canonicalDir(filepath.Dir(q.Output)); err != nil {
		return err
	}
	st, err := os.Lstat(q.Output)
	if err != nil {
		return err
	}
	if stat, ok := st.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		return fmt.Errorf("output hardlink alias")
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("output not regular")
	}
	var c blueprint.Component
	if err = strictDecode(r.Component, &c); err != nil {
		return err
	}
	missing, err := c.CheckOutput(q.Output)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing output headings")
	}
	f, err := os.Open(q.Output)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = syncDirectory(filepath.Dir(q.Output)); err != nil {
		return err
	}
	hash, err := events.HashFile(q.Output)
	if err != nil {
		return err
	}
	q.Result.OutputFile = q.Output
	q.Result.OutputSHA256 = hash
	return nil
}
func syncedEvent(path string, event events.Event) error {
	if err := durableMkdir(filepath.Dir(path)); err != nil {
		return err
	}
	if _, err := events.Append(path, event); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return syncDirectory(filepath.Dir(path))
}
func entityPath(brain, id, epic string) (string, error) {
	ix, err := store.ReadIndex(brain)
	if err != nil {
		return "", err
	}
	entry, err := ix.Lookup(id)
	if err != nil {
		return "", err
	}
	if entry.Epic != epic || !filepath.IsAbs(entry.Dir) {
		return "", fmt.Errorf("event entity outside authoritative epic")
	}
	return store.EventsPath(entry.Dir), nil
}

func scopeEventsPath(brain string, scope Scope) (string, error) {
	id := scope.WorkID
	if id == "" {
		id = scope.EpicID
	}
	return entityPath(brain, id, scope.EpicID)
}

// File-producing helpers drive work-step transitions. Reply-only review turns
// do not claim a work-step lifecycle. This is an execution intent, not an end
// event: the output need not exist yet and has no claimed hash.
func (s Service) publishStart(ctx context.Context, db *ledger, r *jobRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q := r.Rounds[r.Job.Round]
	if q.Output == "" {
		return nil
	}
	if err := s.check(ctx, db, r.Job); err != nil {
		return err
	}
	if !strings.HasPrefix(db.Reservations[q.Output], r.Job.ID+":") {
		return fmt.Errorf("start has no owned output reservation")
	}
	if _, err := canonicalDir(filepath.Dir(q.Output)); err != nil {
		return err
	}
	path, err := scopeEventsPath(s.Brain, r.Job.Scope)
	if err != nil {
		return err
	}
	if err = syncedEvent(path, events.Event{Kind: events.KindStart, Sender: events.SenderProgram, Text: r.Job.Request.Component, File: q.Output, Lease: r.Job.Scope.Lease, Key: fmt.Sprintf("helper:%s:%d:start", r.Job.ID, r.Job.Round)}); err != nil {
		return err
	}
	if s.testCheckpoint != nil {
		s.testCheckpoint("start_published")
	}
	return nil
}

func flush(ctx context.Context, d RunnerDeps) error {
	type delivery struct {
		job        Job
		result     RoundResult
		escalation bool
	}
	var deliveries []delivery
	now := time.Now()
	if d.Now != nil {
		now = d.Now()
	}
	err := d.Service.transaction(ctx, true, func(db *ledger) error {
		for _, r := range db.Jobs {
			for n := 1; n <= r.Job.Round; n++ {
				q := r.Rounds[n]
				if !terminal(q.Result.State) && q.Result.State != "unknown" && q.Result.State != "blocked" {
					continue
				}
				if q.Result.State == "completed" && !checkpointed(q) {
					setRoundState(r, n, "unknown", "checkpoint_unconfirmed")
				}
				if !q.Published {
					if q.Result.State == "completed" {
						if !q.Stopped || !q.Ended {
							return fmt.Errorf("completion without writer barrier")
						}
						if err := d.Service.check(ctx, db, r.Job); err != nil {
							setState(r, "interrupted", "publication_authority_revoked")
							continue
						}
						if q.Result.OutputFile != "" {
							hash, e := events.HashFile(q.Result.OutputFile)
							if e != nil || hash != q.Result.OutputSHA256 {
								return fmt.Errorf("output changed before publication")
							}
							path, e := scopeEventsPath(d.Service.Brain, r.Job.Scope)
							if e != nil {
								return e
							}
							if e = syncedEvent(path, events.Event{Kind: events.KindEnd, Sender: events.SenderProgram, File: q.Result.OutputFile, SHA256: q.Result.OutputSHA256, Key: fmt.Sprintf("helper:%s:%d:end", r.Job.ID, n)}); e != nil {
								return e
							}
							if d.Service.testCheckpoint != nil {
								d.Service.testCheckpoint("completion_published")
							}
						}
					}
					path, e := entityPath(d.Service.Brain, r.Job.Scope.OwnerID, r.Job.Scope.EpicID)
					if e != nil {
						return e
					}
					text := fmt.Sprintf("Helper %s round %d: %s. Read helper result; explicit continuation is required for recovery_required.", r.Job.ID, n, q.Result.State)
					if e = syncedEvent(path, events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: text, Key: fmt.Sprintf("helper:%s:%d:%s", r.Job.ID, n, q.Result.State)}); e != nil {
						return e
					}
					q.Published = true
				}
				if !q.Delivered {
					j := r.Job
					j.Round = n
					j.State = q.Result.State
					deliveries = append(deliveries, delivery{job: j, result: q.Result})
				}
				if q.Result.State == "unknown" && !q.UnknownAt.IsZero() && now.Sub(q.UnknownAt) >= 2*time.Minute {
					if !q.EscalationPublished {
						path, e := entityPath(d.Service.Brain, r.Job.Scope.OwnerID, r.Job.Scope.EpicID)
						if e != nil {
							return e
						}
						if e = syncedEvent(path, events.Event{Kind: events.KindMessage, Sender: events.SenderProgram, Text: fmt.Sprintf("Helper %s round %d remains unknown; fencing or an explicit ownership decision is required.", r.Job.ID, n), Key: fmt.Sprintf("helper:%s:%d:unknown-escalation", r.Job.ID, n)}); e != nil {
							return e
						}
						q.EscalationPublished = true
					}
					if !q.EscalationDelivered {
						j := r.Job
						j.Round = n
						j.State = "unknown"
						result := q.Result
						result.ErrorCode = "writer_unknown_unchanged"
						deliveries = append(deliveries, delivery{job: j, result: result, escalation: true})
					}
				}

			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, v := range deliveries {
		// Notify is a wake hint. The keyed durable inbox above is the source of
		// truth; retries after a lost acknowledgement must be idempotent at receiver.
		if d.Notify == nil {
			return fmt.Errorf("helper notification callback required")
		}
		if err = d.Notify(ctx, v.job, v.result); err != nil {
			continue
		}
		if d.Service.testCheckpoint != nil {
			d.Service.testCheckpoint("delivery_ack")
		}
		err = d.Service.transaction(ctx, true, func(db *ledger) error {
			r, e := lookup(db, v.job.ID)
			if e != nil {
				return e
			}
			q := r.Rounds[v.result.Round]
			if q.Result.State == v.result.State {
				if v.escalation {
					q.EscalationDelivered = true
				} else {
					q.Delivered = true
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if d.Service.testCheckpoint != nil {
			d.Service.testCheckpoint("delivery_recorded")
		}
	}
	return nil
}
