package epic

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
)

// fakeLinks is GitHub's blocked-by links, by "<issue> <blocking id>".
type fakeLinks struct {
	links map[string]bool
	calls []string
}

func (f *fakeLinks) Issue(_ context.Context, repo string, n int) (gh.Issue, error) {
	return gh.Issue{ID: int64(1000 + n), Number: n, RepositoryURL: "https://api.github.com/repos/" + repo}, nil
}

func (f *fakeLinks) BlockedBy(_ context.Context, repo string, n int) ([]gh.Issue, error) {
	var out []gh.Issue
	for k := range f.links {
		issue, id, _ := strings.Cut(k, " ")
		if issue == (Ref{repo, n}).String() {
			n, _ := strconv.ParseInt(id, 10, 64)
			out = append(out, gh.Issue{ID: n})
		}
	}
	return out, nil
}

func key(repo string, n int, id int64) string { return fmt.Sprintf("%s %d", Ref{repo, n}, id) }

func (f *fakeLinks) AddBlockedBy(_ context.Context, repo string, n int, id int64) error {
	f.calls = append(f.calls, "add "+key(repo, n, id))
	f.links[key(repo, n, id)] = true
	return nil
}

func (f *fakeLinks) RemoveBlockedBy(_ context.Context, repo string, n int, id int64) error {
	f.calls = append(f.calls, "remove "+key(repo, n, id))
	delete(f.links, key(repo, n, id))
	return nil
}

func TestIssueLinks(t *testing.T) {
	p := mustParse(t, validPlan)
	foreign := func(id string) (Ref, bool) { return Ref{"anuma-ai/sdk", 7}, id == "e2-sdk-7" }
	got := p.IssueLinks(foreign)
	want := []IssueLink{
		{Issue: "anuma-ai/sdk#42", Blocking: "kingpinXD/factory#12"},
		{Issue: "kingpinXD/factory#13", Blocking: "kingpinXD/factory#12"},
		{Issue: "anuma-ai/sdk#42", Blocking: "anuma-ai/sdk#7"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("IssueLinks = %+v\nwant %+v (the PR blocker is not an item and is left out)", got, want)
	}
}

func TestMirrorLinksTouchesOnlyItsOwnLinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), LinksFile)
	g := &fakeLinks{links: map[string]bool{
		// someone else already linked #13 blocked by #12 (id 1012)
		"kingpinXD/factory#13 1012": true,
	}}
	want := []IssueLink{
		{Issue: "anuma-ai/sdk#42", Blocking: "kingpinXD/factory#12"},
		{Issue: "kingpinXD/factory#13", Blocking: "kingpinXD/factory#12"},
	}
	if err := MirrorLinks(context.Background(), g, path, want); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(g.calls, []string{"add anuma-ai/sdk#42 1012"}) {
		t.Fatalf("calls = %v, want only the link nobody had made", g.calls)
	}
	made, _ := readLinks(path)
	if len(made) != 1 || made[0].Issue != "anuma-ai/sdk#42" || made[0].BlockingID != 1012 {
		t.Fatalf("recorded = %+v, want only the factory's own link", made)
	}

	// The plan drops both links: only the factory's own is removed.
	g.calls = nil
	if err := MirrorLinks(context.Background(), g, path, nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(g.calls, []string{"remove anuma-ai/sdk#42 1012"}) {
		t.Fatalf("calls = %v, want only the factory's own link removed", g.calls)
	}
	if !g.links["kingpinXD/factory#13 1012"] {
		t.Error("the link someone else made was removed")
	}
	if made, _ := readLinks(path); len(made) != 0 {
		t.Errorf("recorded after removal = %+v", made)
	}
}

func TestMirrorLinksTreats422AsLinked(t *testing.T) {
	path := filepath.Join(t.TempDir(), LinksFile)
	fake := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		line := strings.Join(c.Args, " ")
		switch {
		case strings.Contains(line, "-X POST"):
			return []byte(`{"message":"Target issue has already been taken"}`), &proc.Error{Cmd: "gh", Err: errors.New("exit status 1"),
				Stdout: []byte(`{"message":"Target issue has already been taken"}`), Stderr: "gh: Validation Failed (HTTP 422)"}
		case strings.HasSuffix(line, "/dependencies/blocked_by"):
			return []byte("[]"), nil
		case strings.Contains(line, "issues/12"):
			return []byte(`{"id": 5012, "number": 12}`), nil
		}
		return nil, errors.New("unexpected " + line)
	}}
	want := []IssueLink{{Issue: "o/r#13", Blocking: "o/r#12"}}
	if err := MirrorLinks(context.Background(), gh.Client{Runner: fake}, path, want); err != nil {
		t.Fatalf("a 422 on add must count as linked: %v", err)
	}
	if made, _ := readLinks(path); len(made) != 1 || made[0].BlockingID != 5012 {
		t.Fatalf("recorded = %+v", made)
	}
}
