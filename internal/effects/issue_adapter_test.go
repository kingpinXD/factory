package effects

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/gh"
	"github.com/kingpinXD/factory/internal/proc"
)

type issueHub struct {
	issues   map[string]gh.EffectIssue
	comments map[string][]gh.EffectComment
	children map[string][]gh.EffectIssue
	writes   []proc.Cmd
	pending  *proc.Cmd
	lose     Kind
	hidden   bool
	response map[string]string
}

func newIssueHub() *issueHub {
	return &issueHub{issues: map[string]gh.EffectIssue{}, comments: map[string][]gh.EffectComment{}, children: map[string][]gh.EffectIssue{}, response: map[string]string{}}
}
func issueKey(repo string, n int) string { return fmt.Sprintf("%s#%d", strings.ToLower(repo), n) }
func (h *issueHub) add(repo string, n int, node string) gh.EffectIssue {
	i := gh.EffectIssue{ID: int64(n), NodeID: node, Number: n, RepositoryURL: "https://api.github.com/repos/" + repo, URL: fmt.Sprintf("https://github.com/%s/issues/%d", repo, n), Title: "Original", Body: "Original Body", State: "open", UpdatedAt: "2026-11-11T10:00:00Z", User: gh.User{Login: "different-creator"}}
	h.issues[issueKey(repo, n)] = i
	return i
}
func encodeIssueValue(v any) []byte { b, _ := json.Marshal(v); return b }
func cmdField(c proc.Cmd, name string) string {
	for _, arg := range c.Args {
		if strings.HasPrefix(arg, name+"=") {
			return strings.TrimPrefix(arg, name+"=")
		}
	}
	return ""
}
func issueCmdTarget(c proc.Cmd) (method, endpoint string) {
	method = "GET"
	for n, arg := range c.Args {
		if arg == "-X" && n+1 < len(c.Args) {
			method = c.Args[n+1]
		}
		if strings.HasPrefix(arg, "repos/") {
			endpoint = arg
		}
	}
	return method, endpoint
}
func (h *issueHub) respond(c proc.Cmd) ([]byte, error) {
	method, endpoint := issueCmdTarget(c)
	if len(c.Args) == 2 && c.Args[0] == "api" && c.Args[1] == "user" {
		return []byte(`{"login":"factory-bot"}`), nil
	}
	if method != "GET" {
		h.writes = append(h.writes, c)
		kind := IssueCreate
		switch {
		case strings.Contains(endpoint, "/sub_issues"):
			kind = IssueAttach
		case strings.Contains(endpoint, "/comments"):
			kind = IssueComment
		case strings.Contains(endpoint, "/assignees"):
			kind = IssueAssign
		case cmdField(c, "state") == "closed":
			kind = IssueClose
		case method == "PATCH":
			kind = IssueUpdate
		}
		if h.lose == kind && h.pending == nil {
			copy := c
			h.pending = &copy
			return nil, context.DeadlineExceeded
		}
		return h.apply(c)
	}
	if raw, ok := h.response[endpoint]; ok {
		return []byte(raw), nil
	}
	parts := strings.Split(strings.Split(endpoint, "?")[0], "/")
	if len(parts) < 4 {
		return nil, fmt.Errorf("unexpected read %v", c.Args)
	}
	repo := parts[1] + "/" + parts[2]
	if len(parts) == 4 {
		items := []gh.EffectIssue{}
		if !h.hidden {
			keys := mapsKeys(h.issues)
			slices.Sort(keys)
			for _, key := range keys {
				i := h.issues[key]
				if strings.EqualFold(i.Repo(), repo) {
					items = append(items, i)
				}
			}
		}
		return encodeIssueValue(items), nil
	}
	n, err := strconv.Atoi(parts[4])
	if err != nil {
		return nil, err
	}
	key := issueKey(repo, n)
	if len(parts) == 5 {
		i, ok := h.issues[key]
		if !ok {
			return nil, errors.New("missing issue")
		}
		return encodeIssueValue(i), nil
	}
	switch parts[5] {
	case "comments":
		rows := h.comments[key]
		if rows == nil {
			rows = []gh.EffectComment{}
		}
		return encodeIssueValue(rows), nil
	case "sub_issues":
		rows := h.children[key]
		if rows == nil {
			rows = []gh.EffectIssue{}
		}
		return encodeIssueValue(rows), nil
	}
	return nil, fmt.Errorf("unexpected read %v", c.Args)
}
func mapsKeys[M ~map[string]V, V any](m M) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
func (h *issueHub) apply(c proc.Cmd) ([]byte, error) {
	method, endpoint := issueCmdTarget(c)
	parts := strings.Split(endpoint, "/")
	repo := parts[1] + "/" + parts[2]
	if len(parts) == 4 {
		i := h.add(repo, 100, "I_created")
		i.Title, i.Body, i.User = cmdField(c, "title"), cmdField(c, "body"), gh.User{Login: "factory-bot"}
		h.issues[issueKey(repo, 100)] = i
		return encodeIssueValue(i), nil
	}
	n, _ := strconv.Atoi(parts[4])
	key := issueKey(repo, n)
	i := h.issues[key]
	if len(parts) == 6 {
		switch parts[5] {
		case "comments":
			comment := gh.EffectComment{ID: int64(50 + len(h.comments[key])), User: gh.User{Login: "factory-bot"}, Body: cmdField(c, "body"), IssueURL: fmt.Sprintf("https://api.github.com/repos/%s/issues/%d", repo, n)}
			h.comments[key] = append(h.comments[key], comment)
			return encodeIssueValue(comment), nil
		case "assignees":
			i.Assignees = append(i.Assignees, gh.User{Login: cmdField(c, "assignees[]")})
		case "sub_issues":
			childID, _ := strconv.ParseInt(cmdField(c, "sub_issue_id"), 10, 64)
			for _, child := range h.issues {
				if child.ID == childID {
					h.children[key] = append(h.children[key], child)
					return encodeIssueValue(child), nil
				}
			}
			return nil, errors.New("no matching child")
		}
	} else if method == "PATCH" {
		if cmdField(c, "state") == "closed" {
			i.State, i.StateReason = "closed", cmdField(c, "state_reason")
		} else {
			i.Body = cmdField(c, "body")
			i.UpdatedAt = "2026-11-11T10:01:00Z"
		}
	}
	h.issues[key] = i
	return encodeIssueValue(i), nil
}
func (h *issueHub) reveal(t *testing.T) {
	t.Helper()
	if h.pending == nil {
		t.Fatal("no pending mutation")
	}
	if _, err := h.apply(*h.pending); err != nil {
		t.Fatal(err)
	}
	h.hidden = false
}
func issueAdapterFor(h *issueHub) IssueAdapter {
	return IssueAdapter{GitHub: gh.Client{Runner: &proc.Fake{Respond: h.respond}}}
}
func issueTestSlot(kind Kind) Slot {
	s := slot("A")
	s.Operation = kind
	s.Target = Target{Repo: "o/r"}
	if kind != IssueCreate {
		s.Target.Number = 5
	}
	return s
}
func receiptPayload(s Slot, p any) Receipt {
	b, _ := json.Marshal(p)
	canonical, _ := canonicalPayload(b)
	id, _ := s.ID()
	return Receipt{Version: 2, Revision: 1, State: PreparedState, Origin: programOrigin("A"), Prepared: Prepared{Slot: s, SlotID: id, CapabilityVersion: 2, Payload: canonical, PayloadHash: Digest(sha256.Sum256(canonical)).String()}}
}
func createAdapterReceipt() Receipt {
	s := issueTestSlot(IssueCreate)
	id, _ := s.ID()
	marker := IssueMarker(id)
	return receiptPayload(s, IssueCreatePayload{Actor: "factory-bot", Title: "Producer", Body: "Fixture producer\n\n" + marker, Marker: marker})
}
func issueOperationReceipt(kind Kind) Receipt {
	s := issueTestSlot(kind)
	id, _ := s.ID()
	switch kind {
	case IssueCreate:
		return createAdapterReceipt()
	case IssueUpdate:
		return receiptPayload(s, IssueUpdatePayload{Actor: "factory-bot", NodeID: "I_5", SourceBody: "Original Body", SourceUpdatedAt: "2026-11-11T10:00:00Z", Body: "Updated"})
	case IssueComment:
		return receiptPayload(s, IssueCommentPayload{Actor: "factory-bot", NodeID: "I_5", Body: "Comment\n\n" + IssueMarker(id), Marker: IssueMarker(id), ExistingCommentIDs: []int64{}})
	case IssueAssign:
		return receiptPayload(s, IssueAssignPayload{Actor: "factory-bot", NodeID: "I_5"})
	case IssueClose:
		return receiptPayload(s, IssueClosePayload{Actor: "factory-bot", NodeID: "I_5", Reason: "completed"})
	}
	panic("unsupported test kind")
}

func TestIssueAdapterSingleMutationAndPureObservation(t *testing.T) {
	for _, kind := range []Kind{IssueCreate, IssueUpdate, IssueComment, IssueAssign, IssueClose} {
		t.Run(string(kind), func(t *testing.T) {
			h := newIssueHub()
			h.add("O/R", 5, "I_5")
			a := issueAdapterFor(h)
			r := issueOperationReceipt(kind)
			o, err := a.Execute(testContext, r)
			if err != nil || o.Decision != Applied || a.ValidateObservation(r, o) != nil {
				t.Fatalf("execute %s %v %v", kind, o.Decision, err)
			}
			if len(h.writes) != 1 {
				t.Fatalf("writes=%v", h.writes)
			}
			if kind == IssueClose && strings.Contains(strings.Join(h.writes[0].Args, " "), "comment") {
				t.Fatal("close bundled a comment")
			}
			again, err := a.Observe(testContext, r)
			if err != nil || again.Decision != Applied || len(h.writes) != 1 {
				t.Fatalf("observation mutated or missed result: %v %v", again, err)
			}
		})
	}
}

func TestIssueAdapterNegativeObservationNeverPermitsExecute(t *testing.T) {
	for _, kind := range []Kind{IssueCreate, IssueUpdate, IssueComment, IssueAssign, IssueClose} {
		t.Run(string(kind), func(t *testing.T) {
			h := newIssueHub()
			h.add("o/r", 5, "I_5")
			a := issueAdapterFor(h)
			r := issueOperationReceipt(kind)
			if kind == IssueCreate {
				h.hidden = true
			}
			o, err := a.Observe(testContext, r)
			if err != nil || o.Decision != Unresolved || len(h.writes) != 0 {
				t.Fatalf("absent became %s %v writes=%d", o.Decision, err, len(h.writes))
			}
			if a.ValidateObservation(r, Observation{Decision: SafeToExecute}) == nil {
				t.Fatal("no-effect accepted without positive evidence")
			}
		})
	}
}

func TestIssueAdapterSourceEditAndIdentityRefuseBeforeMutation(t *testing.T) {
	for _, change := range []string{"body", "revision", "node", "repo", "actor"} {
		t.Run(change, func(t *testing.T) {
			h := newIssueHub()
			i := h.add("o/r", 5, "I_5")
			r := issueOperationReceipt(IssueUpdate)
			switch change {
			case "body":
				i.Body = "Human edit"
			case "revision":
				i.UpdatedAt = "2026-11-11T10:00:01Z"
			case "node":
				i.NodeID = "I_foreign"
			case "repo":
				i.RepositoryURL = "https://api.github.com/repos/x/y"
			case "actor":
				h.response["user"] = `{"login":"other"}`
			}
			h.issues[issueKey("o/r", 5)] = i
			a := issueAdapterFor(h)
			if change == "actor" {
				a.GitHub.Runner = &proc.Fake{Respond: func(c proc.Cmd) ([]byte, error) {
					if slices.Contains(c.Args, "user") {
						return []byte(`{"login":"other"}`), nil
					}
					return h.respond(c)
				}}
			}
			o, err := a.Execute(testContext, r)
			if err == nil || o.Decision != Unresolved || len(h.writes) != 0 {
				t.Fatalf("changed %s admitted: %s %v writes=%d", change, o.Decision, err, len(h.writes))
			}
			if (change == "body" || change == "revision") && !errors.Is(err, ErrConflict) {
				t.Fatalf("wrong source guard: %v", err)
			}
		})
	}
}

func TestIssueAdapterMarkerUniquenessAcrossEveryPage(t *testing.T) {
	for _, kind := range []Kind{IssueCreate, IssueComment} {
		for _, scenario := range []string{"exact", "exact duplicate", "edited duplicate", "foreign actor", "wrong actor only", "edited only", "repeated marker", "missing", "truncated", "null page"} {
			t.Run(string(kind)+"/"+scenario, func(t *testing.T) {
				h := newIssueHub()
				h.add("o/r", 5, "I_5")
				a := issueAdapterFor(h)
				r := issueOperationReceipt(kind)
				if _, err := a.Execute(testContext, r); err != nil {
					t.Fatal(err)
				}
				if kind == IssueCreate {
					i := h.issues[issueKey("o/r", 100)]
					copy := i
					copy.ID, copy.Number, copy.NodeID = 101, 101, "I_duplicate"
					copy.URL = "https://github.com/o/r/issues/101"
					rows := []gh.EffectIssue{i}
					switch scenario {
					case "exact duplicate":
						rows = append(rows, copy)
					case "wrong actor only":
						rows[0].User.Login = "other"
					case "edited only":
						rows[0].Body = "Edited " + rows[0].Body
					case "edited duplicate":
						copy.Body = "Edited " + copy.Body
						rows = append(rows, copy)
					case "foreign actor":
						copy.User.Login = "other"
						rows = append(rows, copy)
					case "repeated marker":
						rows[0].Body += "\n" + IssueMarker(r.Prepared.SlotID)
					case "missing":
						rows = nil
					}
					raw := string(encodeIssueValue([]gh.EffectIssue{})) + "\n" + string(encodeIssueValue(rows))
					if scenario == "truncated" {
						raw += "\n["
					}
					if scenario == "null page" {
						raw += "\nnull"
					}
					h.response["repos/o/r/issues?state=all&per_page=100"] = raw
				} else {
					c := h.comments[issueKey("o/r", 5)][0]
					copy := c
					copy.ID = 99
					rows := []gh.EffectComment{c}
					switch scenario {
					case "exact duplicate":
						rows = append(rows, copy)
					case "wrong actor only":
						rows[0].User.Login = "other"
					case "edited only":
						rows[0].Body = "Edited " + rows[0].Body
					case "edited duplicate":
						copy.Body = "Edited " + copy.Body
						rows = append(rows, copy)
					case "foreign actor":
						copy.User.Login = "other"
						rows = append(rows, copy)
					case "repeated marker":
						rows[0].Body += "\n" + IssueMarker(r.Prepared.SlotID)
					case "missing":
						rows = nil
					}
					raw := string(encodeIssueValue([]gh.EffectComment{})) + "\n" + string(encodeIssueValue(rows))
					if scenario == "truncated" {
						raw += "\n["
					}
					if scenario == "null page" {
						raw += "\nnull"
					}
					h.response["repos/o/r/issues/5/comments?per_page=100"] = raw
				}
				o, err := a.Observe(testContext, r)
				if scenario == "exact" {
					if err != nil || o.Decision != Applied {
						t.Fatalf("complete second page rejected: %s %v", o.Decision, err)
					}
				} else if o.Decision != Unresolved {
					t.Fatalf("ambiguous %s accepted", scenario)
				}
				if len(h.writes) != 1 {
					t.Fatal("observer sent another mutation")
				}
			})
		}
	}
}

func prepareIssueService(t *testing.T, w *testWorld, r Receipt, origin Origin) Receipt {
	t.Helper()
	w.a.mu.Lock()
	defer w.a.mu.Unlock()
	prepared, err := w.s.Prepare(testContext, origin, r.Prepared.Slot, Proposal{Version: 2, Kind: r.Prepared.Slot.Operation, Payload: r.Prepared.Payload}, artifact())
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}
func configureIssueService(w *testWorld, a IssueAdapter) {
	for _, kind := range []Kind{IssueCreate, IssueAttach, IssueUpdate, IssueComment, IssueAssign, IssueClose} {
		w.s.Adapters[kind] = a
	}
}

func TestIssueAdapterLostRepliesUseRealJournalAndAdoptDelayedSuccess(t *testing.T) {
	for _, kind := range []Kind{IssueCreate, IssueUpdate, IssueComment, IssueAssign, IssueClose} {
		t.Run(string(kind), func(t *testing.T) {
			h := newIssueHub()
			h.add("o/r", 5, "I_5")
			h.lose = kind
			a := issueAdapterFor(h)
			w := world(t)
			configureIssueService(w, a)
			r := w.claim(t, prepareIssueService(t, w, issueOperationReceipt(kind), programOrigin("A")), "writer")
			r, err := w.s.Dispatch(testContext, r.Claim, r)
			if !errors.Is(err, context.DeadlineExceeded) || r.State != UnresolvedState || r.DispatchCount != 1 {
				t.Fatalf("lost ack=%s %v", r.State, err)
			}
			if _, err := w.s.Dispatch(testContext, r.Claim, r); !errors.Is(err, ErrUnresolved) {
				t.Fatalf("redispatch=%v", err)
			}
			o, err := w.s.Observe(testContext, r.Claim, r)
			if err != nil || o.Decision != Unresolved {
				t.Fatalf("negative read=%s %v", o.Decision, err)
			}
			r, _ = w.s.Result(testContext, r.Prepared.SlotID)
			if r.NextCheckAt.IsZero() {
				t.Fatal("unresolved action lost next observation")
			}
			r = w.close(t, r)
			r = w.claim(t, r, "observer")
			h.reveal(t)
			o, err = w.s.Observe(testContext, r.Claim, r)
			if err != nil || o.Decision != Applied {
				t.Fatalf("delayed result=%s %v", o.Decision, err)
			}
			final, _ := w.s.Result(testContext, r.Prepared.SlotID)
			if final.State != AppliedState || len(h.writes) != 1 || final.DispatchCount != 1 {
				t.Fatalf("result=%s mutations=%d", final.State, len(h.writes))
			}
		})
	}
}

func attachReceipt(t *testing.T, created Receipt) Receipt {
	t.Helper()
	s := issueTestSlot(IssueAttach)
	s.Target = Target{Repo: "o/r", Number: 100, ParentRepo: "p/parent", ParentNumber: 7}
	ref := actionRef(created)
	r := receiptPayload(s, IssueAttachPayload{ParentNodeID: "I_parent", Created: created, CreateRef: ref})
	r.Origin.ParentAction = &ref
	return r
}
func TestIssueAdapterCreateAndAttachHaveSeparateActualReceipts(t *testing.T) {
	h := newIssueHub()
	h.add("P/Parent", 7, "I_parent")
	a := issueAdapterFor(h)
	w := world(t)
	configureIssueService(w, a)
	create := w.claim(t, prepareIssueService(t, w, createAdapterReceipt(), programOrigin("A")), "create")
	create, err := w.s.Dispatch(testContext, create.Claim, create)
	if err != nil || create.State != AppliedState || len(h.writes) != 1 || len(h.children) != 0 {
		t.Fatalf("creation bundled attachment: %s %v", create.State, err)
	}
	ar := attachReceipt(t, create)
	attach := w.claim(t, prepareIssueService(t, w, ar, ar.Origin), "attach")
	h.lose = IssueAttach
	attach, err = w.s.Dispatch(testContext, attach.Claim, attach)
	if !errors.Is(err, context.DeadlineExceeded) || attach.State != UnresolvedState || len(h.writes) != 2 {
		t.Fatalf("attach=%s %v", attach.State, err)
	}
	o, err := w.s.Observe(testContext, attach.Claim, attach)
	if err != nil || o.Decision != Unresolved {
		t.Fatalf("missing membership=%s %v", o.Decision, err)
	}
	attach, _ = w.s.Result(testContext, attach.Prepared.SlotID)
	if _, err := w.s.Dispatch(testContext, attach.Claim, attach); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("uncertain attach replay=%v", err)
	}
	h.reveal(t)
	o, err = w.s.Observe(testContext, attach.Claim, attach)
	if err != nil || o.Decision != Applied || len(h.writes) != 2 {
		t.Fatalf("membership=%s %v mutations=%d", o.Decision, err, len(h.writes))
	}
	if o.Resource.Issue.ParentNodeID != "I_parent" || o.Resource.Issue.NodeID != "I_created" {
		t.Fatalf("unbound membership %+v", o.Resource.Issue)
	}
}

func TestIssueAdapterAttachmentRejectsWrongHistoricalCreateProof(t *testing.T) {
	h := newIssueHub()
	h.add("p/parent", 7, "I_parent")
	a := issueAdapterFor(h)
	create := createAdapterReceipt()
	o, err := a.Execute(testContext, create)
	if err != nil {
		t.Fatal(err)
	}
	create.State, create.Resource, create.Evidence = AppliedState, o.Resource, o.Proof
	for _, change := range []string{"receipt", "state", "origin", "origin foreign", "target", "parent", "membership duplicate"} {
		t.Run(change, func(t *testing.T) {
			r := attachReceipt(t, create)
			if change == "membership duplicate" {
				child := h.issues[issueKey("o/r", 100)]
				h.children[issueKey("p/parent", 7)] = []gh.EffectIssue{child, child}
				o, err := a.Observe(testContext, r)
				if err != nil || o.Decision != Unresolved {
					t.Fatalf("duplicate membership=%s %v", o.Decision, err)
				}
				return
			}
			var p IssueAttachPayload
			_ = json.Unmarshal(r.Prepared.Payload, &p)
			switch change {
			case "receipt":
				p.Created.Resource.Issue.NodeID = "I_other"
			case "state":
				p.Created.State = UnresolvedState
			case "origin":
				r.Origin.ParentAction = nil
			case "origin foreign":
				copy := *r.Origin.ParentAction
				copy.SlotID = "foreign"
				r.Origin.ParentAction = &copy
			case "target":
				r.Prepared.Slot.Target.Number = 99
			case "parent":
				p.ParentNodeID = "I_foreign"
			}
			origin := r.Origin
			r = receiptPayload(r.Prepared.Slot, p)
			r.Origin = origin
			before := len(h.writes)
			o, err := a.Execute(testContext, r)
			if err == nil || o.Decision != Unresolved || len(h.writes) != before {
				t.Fatalf("bad %s admitted %s %v", change, o.Decision, err)
			}
		})
	}
}

func TestIssueAdapterRejectsMalformedPayloadAndObservation(t *testing.T) {
	a := IssueAdapter{}
	original := createAdapterReceipt()
	for _, bad := range []string{`null`, `{"actor":"factory-bot","actor":"other"}`, `{"actor":"factory-bot","command":"push"}`} {
		r := original
		r.Prepared.Payload = []byte(bad)
		r.Prepared.PayloadHash = Digest(sha256.Sum256([]byte(bad))).String()
		if err := ValidateIssuePayload(r.Prepared, r.Origin); err == nil {
			t.Fatalf("bad payload accepted %s", bad)
		}
	}
	h := newIssueHub()
	a = issueAdapterFor(h)
	o, err := a.Execute(testContext, original)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"repo", "actor", "body", "node", "proof", "kind"} {
		t.Run(field, func(t *testing.T) {
			changed := copyValue(o)
			switch field {
			case "repo":
				changed.Resource.Issue.Repo = "x/y"
			case "actor":
				changed.Resource.Issue.Actor = "other"
			case "body":
				changed.Resource.Issue.Body = "changed"
			case "node":
				changed.Resource.Issue.NodeID = ""
			case "proof":
				changed.Proof.ObservationSHA256 = Digest{}
			case "kind":
				changed.Proof.Operation = IssueComment
			}
			changed.Proof.ResourceSHA256 = digest(changed.Resource)
			if a.ValidateObservation(original, changed) == nil {
				t.Fatalf("accepted wrong %s", field)
			}
		})
	}
}

func TestIssueAdapterCreateReceiptHashIsCheckedLocally(t *testing.T) {
	h := newIssueHub()
	a := issueAdapterFor(h)
	create := createAdapterReceipt()
	o, err := a.Execute(testContext, create)
	if err != nil {
		t.Fatal(err)
	}
	create.State, create.Resource, create.Evidence = AppliedState, o.Resource, o.Proof
	r := attachReceipt(t, create)
	var p IssueAttachPayload
	_ = json.Unmarshal(r.Prepared.Payload, &p)
	p.Created.Prepared.SourceArtifact = "forged-metadata"
	origin := r.Origin
	r = receiptPayload(r.Prepared.Slot, p)
	r.Origin = origin
	if ValidateIssuePayload(r.Prepared, r.Origin) == nil {
		t.Fatal("altered historical receipt was accepted before admission")
	}
}

func TestIssueAdapterRefusesIncompleteSecondRow(t *testing.T) {
	for _, kind := range []Kind{IssueCreate, IssueComment} {
		t.Run(string(kind), func(t *testing.T) {
			h := newIssueHub()
			h.add("o/r", 5, "I_5")
			a := issueAdapterFor(h)
			r := issueOperationReceipt(kind)
			if _, err := a.Execute(testContext, r); err != nil {
				t.Fatal(err)
			}
			if kind == IssueCreate {
				h.response["repos/o/r/issues?state=all&per_page=100"] = string(encodeIssueValue([]gh.EffectIssue{h.issues[issueKey("o/r", 100)]})) + "\n[{}]"
			} else {
				h.response["repos/o/r/issues/5/comments?per_page=100"] = string(encodeIssueValue(h.comments[issueKey("o/r", 5)])) + "\n[{}]"
			}
			o, _ := a.Observe(testContext, r)
			if o.Decision != Unresolved {
				t.Fatal("incomplete second row certified unique remote outcome")
			}
			if len(h.writes) != 1 {
				t.Fatal("incomplete observation mutated")
			}
		})
	}
}

func TestIssueAdapterRefusesMissingBodySecondRow(t *testing.T) {
	for _, kind := range []Kind{IssueCreate, IssueComment} {
		t.Run(string(kind), func(t *testing.T) {
			h := newIssueHub()
			h.add("o/r", 5, "I_5")
			a := issueAdapterFor(h)
			r := issueOperationReceipt(kind)
			if _, err := a.Execute(testContext, r); err != nil {
				t.Fatal(err)
			}
			var first []byte
			var other map[string]json.RawMessage
			endpoint := "repos/o/r/issues?state=all&per_page=100"
			if kind == IssueCreate {
				i := h.issues[issueKey("o/r", 100)]
				first = encodeIssueValue([]gh.EffectIssue{i})
				i.ID, i.Number, i.NodeID = 101, 101, "I_101"
				i.URL = "https://github.com/o/r/issues/101"
				_ = json.Unmarshal(encodeIssueValue(i), &other)
			} else {
				comments := h.comments[issueKey("o/r", 5)]
				first = encodeIssueValue(comments)
				copy := comments[0]
				copy.ID = 99
				_ = json.Unmarshal(encodeIssueValue(copy), &other)
				endpoint = "repos/o/r/issues/5/comments?per_page=100"
			}
			delete(other, "body")
			h.response[endpoint] = string(first) + "\n[" + string(encodeIssueValue(other)) + "]"
			o, _ := a.Observe(testContext, r)
			if o.Decision != Unresolved {
				t.Fatal("missing second body was treated as a known non-marker row")
			}
			if len(h.writes) != 1 {
				t.Fatal("incomplete observation mutated")
			}
		})
	}
}
