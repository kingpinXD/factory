package gh

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/proc"
)

type publicationTransport func(*http.Request) (*http.Response, error)

func (f publicationTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

const publicationWire = `{"id":1,"node_id":"PR_1","number":9,"html_url":"https://github.com/o/r/pull/9","state":"open","draft":true,"user":{"login":"me"},"title":"Fix item","body":"body","head":{"ref":"factory/e1-w1","sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","repo":{"full_name":"o/r"}},"base":{"ref":"main","sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","repo":{"full_name":"o/r"}},"created_at":"2026-11-16T12:00:00Z","merged_at":null}`

func TestPublicationJSONBodyAndReadyUseProgramWriter(t *testing.T) {
	body := "First line\n\nCloses #9\nLast line\n"
	writes := 0
	transport := publicationTransport(func(r *http.Request) (*http.Response, error) {
		writes++
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatal("non JSON body")
		}
		data, _ := io.ReadAll(r.Body)
		if writes == 1 {
			var payload struct {
				Body  string
				Draft bool
			}
			if err := json.Unmarshal(data, &payload); err != nil || payload.Body != body || !payload.Draft {
				t.Fatalf("body changed %q %v", payload.Body, err)
			}
			return &http.Response{StatusCode: 201, Body: io.NopCloser(strings.NewReader(publicationWire))}, nil
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"data":{"markPullRequestReadyForReview":{"pullRequest":{"id":"PR_1","isDraft":false}}}}`))}, nil
	})
	fake := &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
		raw := publicationWire
		if writes == 2 {
			raw = strings.Replace(raw, `"draft":true`, `"draft":false`, 1)
		}
		return []byte(raw), nil
	}}
	cl := PublicationClient{Reader: Client{Runner: fake}, Writer: ProgramWriter{token: "fake", client: &http.Client{Transport: transport}}}
	req := PublicationRequest{Repo: "o/r", Branch: "factory/e1-w1", Base: "main", Title: "Fix item", Body: body}
	p, err := cl.CreateDraftPR(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	p, err = cl.ReadyPR(context.Background(), req.Repo, p)
	if err != nil || p.Draft || writes != 2 {
		t.Fatalf("ready %+v %v writes=%d", p, err, writes)
	}
}
func TestPublicationLookupRejectsIncompletePages(t *testing.T) {
	for _, raw := range []string{"[" + publicationWire + "]\n[", "null", "[{}]", strings.Replace("["+publicationWire+"]", `"created_at":"2026-11-16T12:00:00Z",`, "", 1)} {
		cl := PublicationClient{Reader: Client{Runner: &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) { return []byte(raw), nil }}}}
		if _, err := cl.PullRequests(context.Background(), "o/r"); err == nil {
			t.Fatal("incomplete lookup accepted", raw)
		}
	}
	fake := &proc.Fake{Respond: func(proc.Cmd) ([]byte, error) { return []byte("[" + publicationWire + "]\n[]"), nil }}
	prs, err := (PublicationClient{Reader: Client{Runner: fake}}).PullRequests(context.Background(), "o/r")
	if err != nil || len(prs) != 1 || prs[0].CreatedAt.IsZero() || prs[0].NodeID != "PR_1" {
		t.Fatalf("lookup %+v %v", prs, err)
	}
	if got := strings.Join(fake.Calls()[0].Args, " "); got != "api --paginate repos/o/r/pulls?state=all&per_page=100" {
		t.Fatal(got)
	}
}
