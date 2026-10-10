package reconcile

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kingpinXD/factory/internal/git"
	"github.com/kingpinXD/factory/internal/store"
)

// Every session prompt opens with the session's task id and names every
// path absolutely: the session's working folder is an empty run folder.

// startPrompt is a session's first prompt: everything its owner holds for it now.
func (r *run) startPrompt(owner *entity, component string) string {
	switch component {
	case componentPlanner:
		return r.plannerPrompt(owner, r.deliveries(owner))
	case componentOrchestrator:
		return r.orchestratorPrompt(owner, "You are starting on this set.", r.workingItems(owner), r.deliveries(owner))
	case babysitterComponent:
		return r.babysitterPromptFor(owner, r.deliveries(owner))
	}
	return taskLine(owner.id)
}

// wakePrompt is what a running or resumed session is told: only what is new.
func (r *run) wakePrompt(owner *entity, component string, news []delivery) string {
	switch component {
	case componentPlanner:
		return r.plannerPrompt(owner, news)
	case componentOrchestrator:
		var items []*entity
		for _, d := range news {
			if d.item != nil {
				items = append(items, d.item)
			}
		}
		return r.orchestratorPrompt(owner, "New work for your set.", items, news)
	case babysitterComponent:
		return r.babysitterPromptFor(owner, news)
	}
	return taskLine(owner.id) + messagesSection(owner.id, news)
}

func taskLine(id string) string { return "factory-task: " + id + "\n" }

// plannerPrompt names the epic, its folder, its input and any messages.
func (r *run) plannerPrompt(epic *entity, msgs []delivery) string {
	in := epic.epic
	var b strings.Builder
	b.WriteString(taskLine(epic.id))
	fmt.Fprintf(&b, "epic folder: %s\n", epic.entry.Dir)
	fmt.Fprintf(&b, "inputs: %s\n", store.InputsPath(epic.entry.Dir))
	fmt.Fprintf(&b, "input (%s): %s\n", in.Kind, in.Input)
	if in.Path != "" {
		fmt.Fprintf(&b, "plan file: %s\n", in.Path)
	}
	if in.RepoHint != "" {
		fmt.Fprintf(&b, "repo hint: %s\n", in.RepoHint)
	}
	b.WriteString(messagesSection(epic.id, msgs))
	return b.String()
}

// orchestratorPrompt names the set, its lease, the epic folder, each item
// with its worktree, and any messages, instructions included.
func (r *run) orchestratorPrompt(set *entity, lead string, items []*entity, msgs []delivery) string {
	var b strings.Builder
	b.WriteString(taskLine(set.id))
	fmt.Fprintf(&b, "lease: %d\n", set.st.Lease)
	if epic := r.ents[set.entry.Epic]; epic != nil {
		fmt.Fprintf(&b, "epic folder: %s\n", epic.entry.Dir)
	}
	fmt.Fprintf(&b, "set folder: %s\n\n%s\n", set.entry.Dir, lead)
	if len(items) > 0 {
		b.WriteString("\nItems, each in the worktree the program made for it:\n")
	}
	for _, it := range items {
		ready, _ := worktreeReady(it)
		w := it.work
		fmt.Fprintf(&b, "- %s (%s): issue %s#%d, repo %s, base %s, branch %s, worktree %s, item folder %s\n",
			it.id, it.cur.State, w.Repo, w.Issue, w.Repo, w.Base, git.Branch(it.id), ready.Text, it.entry.Dir)
	}
	b.WriteString(messagesSection(set.id, msgs))
	return b.String()
}

// workingItems returns the set's open items whose worktree is ready and
// still the orchestrator's: not handed over to a babysitter.
func (r *run) workingItems(set *entity) []*entity {
	items, _ := r.itemsOf(set)
	var out []*entity
	for _, it := range items {
		if _, ok := worktreeReady(it); ok && it.open() && !handedOver(it) {
			out = append(out, it)
		}
	}
	return out
}

// messagesSection lists the messages among ds, each with its seq.
func messagesSection(id string, ds []delivery) string {
	var b strings.Builder
	for _, d := range ds {
		if d.item != nil {
			continue
		}
		if b.Len() == 0 {
			fmt.Fprintf(&b, "\nMessages: act on each, then ack it with `factory inbox %s --ack <seq>`:\n", id)
		}
		fmt.Fprintf(&b, "- seq %d, %s from %s: %s\n", d.msg.Seq, d.msg.Kind, d.msg.Sender, d.msg.Text)
	}
	return b.String()
}

// babysitterWake is what wakes a babysitter (TODO 12), for its prompt.
type babysitterWake struct {
	WorkID, PRURL, Repo, Issue, Branch, Base, Worktree, ItemDir string
	// Reasons say what woke it: a new thread, review, comment or change
	// request, red checks by SHA, a conflict, or "N unanswered threads".
	Reasons []string
	// DMed are the logins already DMed about this PR, from its done events.
	DMed []string
}

// babysitterPrompt starts or wakes a babysitter on one PR.
func babysitterPrompt(w babysitterWake) string {
	var b strings.Builder
	b.WriteString(taskLine(w.WorkID))
	fmt.Fprintf(&b, "PR: %s\nrepo: %s\nissue: %s\nbranch: %s\nbase: %s\n", w.PRURL, w.Repo, w.Issue, w.Branch, w.Base)
	fmt.Fprintf(&b, "worktree (handed over to you): %s\n", w.Worktree)
	fmt.Fprintf(&b, "item folder: %s\n", w.ItemDir)
	fmt.Fprintf(&b, "your pass report: %s\n", filepath.Join(w.ItemDir, "babysit.v<n>.md"))
	fmt.Fprintf(&b, "woken by: %s\n", strings.Join(w.Reasons, "; "))
	dmed := "nobody yet"
	if len(w.DMed) > 0 {
		dmed = strings.Join(w.DMed, ", ")
	}
	fmt.Fprintf(&b, "already DMed about this PR: %s\n", dmed)
	return b.String()
}
