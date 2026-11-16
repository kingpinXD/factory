package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/issue"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/repos"
	"github.com/kingpinXD/factory/internal/session"
	"github.com/kingpinXD/factory/internal/store"
)

var coordinatorGitHub = func() gh.Client { return gh.Client{Runner: proc.Exec{}} }

type coordinatorIssue struct {
	Repo     string `json:"repo"`
	Base     string `json:"base,omitempty"`
	Number   int    `json:"number"`
	Key      string `json:"key"`
	Title    string `json:"title"`
	Body     string `json:"body"`
	Parent   string `json:"parent"`
	Evidence string `json:"evidence"`
}

type unregisteredCoordinatorBaseError struct{}

func (*unregisteredCoordinatorBaseError) Error() string { return "branch is not registered" }

func coordinatorRepo(brain string, ix store.Index, spec session.Spec, repo, base string) (repos.Repo, string, error) {
	bad := func(err error) (repos.Repo, string, error) { return repos.Repo{}, "", err }
	if repo == "" || strings.Count(repo, "/") != 1 || strings.ContainsAny(repo, "\r\n\x00") {
		return bad(fmt.Errorf("repository required"))
	}
	registered, err := repos.ByFullName(brain, repo)
	if err != nil {
		return bad(err)
	}
	b, err := blueprint.Load(blueprint.Path(brain))
	if err != nil {
		return bad(err)
	}
	if slices.Contains(b.Deny.Repos, registered.Name) || slices.Contains(b.Deny.Repos, registered.FullName) {
		return bad(fmt.Errorf("repository is denied"))
	}
	if base == "" {
		base = registered.Default
	}
	if !slices.Contains(registered.Branches, base) {
		return bad(&unregisteredCoordinatorBaseError{})
	}
	for _, branch := range []string{registered.Default, base} {
		if slices.Contains(b.Deny.Branches, blueprint.RepoBranch{Repo: registered.Name, Branch: branch}) {
			return bad(fmt.Errorf("repository branch is denied"))
		}
	}
	owner, err := ix.Lookup(spec.Task)
	if err != nil {
		return bad(err)
	}
	if owner.Kind == blueprint.MachineEpic && strings.HasSuffix(spec.Name, ":planner") {
		return registered, base, nil
	}
	for id, entry := range ix {
		if entry.Epic != spec.EpicID || entry.Kind != blueprint.MachineWork {
			continue
		}
		var work reconcile.WorkInputs
		if err = store.ReadInputs(entry.Dir, &work); err != nil {
			return bad(err)
		}
		if (id == spec.Task || work.Set == spec.Task) && work.Repo == registered.FullName && work.Base == base {
			return registered, base, nil
		}
	}
	return bad(fmt.Errorf("repository is outside the coordinator scope"))
}

func coordinatorIssueControl(ctx context.Context, brain string, ix store.Index, spec session.Spec, req controlRequest) (any, error) {
	var p coordinatorIssue
	if err := strictInput(req.Payload, &p); err != nil {
		return nil, err
	}
	registered, base, err := coordinatorRepo(brain, ix, spec, p.Repo, p.Base)
	if err != nil {
		return nil, err
	}
	p.Repo = registered.FullName
	if req.Op == "issue_create" && p.Parent != "" {
		parent, err := epic.ParseRefIn(p.Parent, p.Repo)
		if err != nil {
			return nil, err
		}
		parentRepo, parentBase, err := coordinatorRepo(brain, ix, spec, parent.Repo, "")
		if err != nil {
			return nil, err
		}
		parentChoice := p
		parentChoice.Key = "parent:" + p.Key
		parentChoice.Parent = ""
		if err = persistCoordinatorRepoChoice(brain, spec, "issue_create", parentChoice, parentRepo, parentBase); err != nil {
			return nil, err
		}
	}
	if req.Op != "issue_read" && req.Op != "issue_children" && req.Op != "query" {
		if err = persistCoordinatorRepoChoice(brain, spec, req.Op, p, registered, base); err != nil {
			return nil, err
		}
	}
	if len(p.Body) > controlLimit || len(p.Title) > 16384 {
		return nil, fmt.Errorf("issue payload exceeds limit")
	}
	cl := coordinatorGitHub()
	owner, _ := ix.Lookup(spec.Task)
	w := issue.Writer{GitHub: cl, Brain: brain, Log: store.EventsPath(owner.Dir), Sender: spec.Task, Epic: spec.EpicID, Now: time.Now}
	switch req.Op {
	case "issue_read":
		return cl.Issue(ctx, p.Repo, p.Number)
	case "issue_children":
		return cl.SubIssueTree(ctx, p.Repo, p.Number)
	case "issue_create":
		return w.Create(ctx, p.Repo, p.Key, p.Title, p.Body, p.Parent)
	case "issue_update":
		return map[string]bool{"updated": true}, w.Update(ctx, p.Repo, p.Number, p.Body)
	case "issue_comment":
		return map[string]bool{"commented": true}, w.Comment(ctx, p.Repo, p.Number, p.Body)
	case "issue_close":
		return map[string]bool{"closed": true}, w.Close(ctx, p.Repo, p.Number, p.Evidence, p.Body)
	case "issue_assign":
		return map[string]bool{"assigned": true}, w.Assign(ctx, p.Repo, p.Number)
	case "query":
		items, err := inFlight(ix, p.Repo)
		if err != nil {
			return nil, err
		}
		out := []map[string]any{}
		for _, it := range items {
			out = append(out, map[string]any{"id": it.id, "epic": it.epic, "state": it.state, "issue": it.issue, "files": it.files})
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported issue operation")
}

type coordinatorRepoChoice struct {
	Version       int    `json:"version"`
	Task          string `json:"task"`
	Session       string `json:"session"`
	Generation    string `json:"generation"`
	SelectionHash string `json:"selection_hash"`
	Key           string `json:"key"`
	Repo          string `json:"repo"`
	Base          string `json:"base"`
	Clone         string `json:"clone"`
	Parent        string `json:"parent,omitempty"`
}

func persistCoordinatorRepoChoice(brain string, spec session.Spec, op string, p coordinatorIssue, registered repos.Repo, base string) error {
	key := "repo:" + registered.FullName
	if op == "issue_create" {
		if p.Key == "" {
			return fmt.Errorf("issue creation needs a stable key")
		}
		key = "create:" + p.Key
	}
	choice := coordinatorRepoChoice{Version: 1, Task: spec.Task, Session: spec.Name, Generation: spec.Generation, SelectionHash: spec.SelectionHash, Key: key, Repo: registered.FullName, Base: base, Clone: registered.Path, Parent: p.Parent}
	if choice.Task == "" || choice.Generation == "" || choice.SelectionHash == "" {
		return fmt.Errorf("repository choice needs an immutable coordinator binding")
	}
	nameHash := sha256.Sum256([]byte(spec.Name + "\x00" + spec.Generation + "\x00" + key))
	dir := filepath.Join(store.Factory(brain), "codex", "repository-choices")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	target := filepath.Join(dir, hex.EncodeToString(nameHash[:])+".json")
	if data, err := os.ReadFile(target); err == nil {
		var prior coordinatorRepoChoice
		if err = strictInput(data, &prior); err != nil {
			return err
		}
		if !reflect.DeepEqual(prior, choice) {
			return fmt.Errorf("recorded repository choice conflicts with retry")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	data, err := json.Marshal(choice)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".choice-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Link(f.Name(), target); err != nil {
		return err
	}
	for parent := dir; ; parent = filepath.Dir(parent) {
		d, err := os.Open(parent)
		if err != nil {
			return err
		}
		err = d.Sync()
		d.Close()
		if err != nil {
			return err
		}
		if parent == store.Factory(brain) {
			break
		}
	}
	return nil
}
