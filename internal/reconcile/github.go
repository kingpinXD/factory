package reconcile

import (
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/git"
)

// reconcileGitHub reads, on a full reconcile, the pull request of each open
// work item, so its github moves can be checked.
func (r *run) reconcileGitHub() {
	for _, id := range r.order() {
		if e := r.ents[id]; e.work != nil && e.open() {
			if err := r.readPR(e); err != nil {
				r.fail("%s: %v", id, err)
			}
		}
	}
}

// readPR reads the item's PR: the recorded or adopted one by number, else
// the newest one from the item's own branch once it has started. Only a PR
// whose head is the item's factory/<work-id> branch is ever recorded; gh
// leaves out PRs from forks.
func (r *run) readPR(e *entity) error {
	ctx, cancel := r.call()
	defer cancel()
	if n := max(e.st.PR, e.work.PR); n != 0 {
		pr, err := r.d.GitHub.PR(ctx, e.work.Repo, n)
		if err == nil {
			r.record(e, pr)
		}
		return err
	}
	if e.cur.State == e.m.Start {
		return nil
	}
	branch := git.Branch(e.id)
	pr, found, err := r.d.GitHub.PRByBranch(ctx, e.work.Repo, branch)
	if err != nil || !found || pr.HeadRefName != branch {
		return err
	}
	r.record(e, pr)
	return nil
}

func (r *run) record(e *entity, pr gh.PR) {
	if e.st.PR != pr.Number {
		r.say("%s: recorded PR %s", e.id, pr.URL)
	}
	e.pr = &pr
	e.st.PR, e.st.PRURL, e.st.PRState, e.st.HeadSHA = pr.Number, pr.URL, pr.State, pr.HeadRefOid
}

// baseGreen reports whether repo's base branch is not red, read once per
// tick.
func (r *run) baseGreen(repo, base string) (bool, error) {
	k := repo + "@" + base
	if res, ok := r.bases[k]; ok {
		return res.green, res.err
	}
	ctx, cancel := r.call()
	defer cancel()
	green, err := r.d.GitHub.BaseGreen(ctx, repo, base)
	r.bases[k] = baseResult{green, err}
	return green, err
}
