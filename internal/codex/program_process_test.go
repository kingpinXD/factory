package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/effects"
)

func processDigest(s string) effects.Digest { return sha256.Sum256([]byte(s)) }
func processRecordForTest(subject ProgramProcessSubject, mode string) ProgramProcessRecord {
	id, _ := ProgramProcessSubjectID(subject)
	admitted := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	r := ProgramProcessRecord{Schema: 1, Revision: 1, Subject: subject, SubjectID: id, ClaimID: id + "." + strings.Repeat("a", 32), NonceSHA256: processDigest("nonce"), OwnerID: "owner", LogicalRevision: "authority-1", InputSHA256: processDigest("input"), FactorySHA256: processDigest("binary"), Mode: mode, AdmittedAt: admitted, Deadline: admitted.Add(10 * time.Second), SelectionRevision: 1, Stage: "prepared"}
	if subject.ProgramRead != nil {
		r.InputSHA256 = subject.ProgramRead.SHA256
	}
	if subject.AcceptanceRead != nil {
		r.InputSHA256 = subject.AcceptanceRead.SHA256
	}
	r.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(r)
	r.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(r))
	return r
}
func closedProcessForTest(r ProgramProcessRecord, identity ProcessIdentity) ProgramProcessRecord {
	r.Stage = "closed"
	r.Host = identity
	r.SealedRevision = 3
	r.Revision = 4
	kind := "owned_group_gone"
	if identity == (ProcessIdentity{}) {
		r.SealedRevision = 2
		r.Revision = 3
		kind = "never_claimed"
	}
	r.ClosedProof = &ProgramProcessClosedProof{Kind: kind, SubjectID: r.SubjectID, ClaimID: r.ClaimID, SealedRevision: r.SealedRevision, EnrollmentSHA256: r.EnrollmentSHA256, SelectionSHA256: r.SelectionSHA256, Host: identity, Deadline: r.Deadline}
	return r
}
func writeProcessForTest(t *testing.T, brain string, r ProgramProcessRecord) ProgramProcessRef {
	t.Helper()
	p, err := ProgramProcessPath(brain, r.SubjectID, r.ClaimID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	return ProgramProcessRef{SubjectID: r.SubjectID, ClaimID: r.ClaimID, Revision: r.Revision, SHA256: sha256.Sum256(b)}
}
func canonicalProcessBrain(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestProgramProcessSubjectsHaveOneActualIdentity(t *testing.T) {
	slot := strings.Repeat("1", 64)
	ref := ProgramReadRef{RequestID: strings.Repeat("2", 64), SHA256: processDigest("request")}
	grant := AcceptanceReadGrantRef{Binding: strings.Repeat("3", 64), ProofID: "proof-private", SHA256: processDigest("grant")}
	cases := []ProgramProcessSubject{{Kind: "normal_effect", EffectSlotID: slot}, {Kind: "program_read", ProgramRead: &ref}, {Kind: "acceptance_read", AcceptanceRead: &grant}}
	ids := map[string]bool{}
	for _, s := range cases {
		id, err := ProgramProcessSubjectID(s)
		if err != nil {
			t.Fatal(err)
		}
		if ids[id] {
			t.Fatal("subject kinds shared identity")
		}
		ids[id] = true
		if s.Kind == "normal_effect" && id != slot {
			t.Fatal("normal effect lost its exact E1 identity")
		}
		if err := ValidateProgramProcessRecord(processRecordForTest(s, map[bool]string{true: "execute", false: "read"}[s.Kind == "normal_effect"])); err != nil {
			t.Fatal(err)
		}
	}
	for _, s := range []ProgramProcessSubject{{Kind: "program_read", EffectSlotID: slot, ProgramRead: &ref}, {Kind: "normal_effect", EffectSlotID: slot, AcceptanceRead: &grant}, {Kind: "acceptance_read", ProgramRead: &ref, AcceptanceRead: &grant}, {Kind: "unknown", EffectSlotID: slot}} {
		if _, err := ProgramProcessSubjectID(s); err == nil {
			t.Fatalf("ambiguous subject accepted: %s", s.Kind)
		}
	}
	changed := ref
	changed.SHA256 = processDigest("different-input")
	id, _ := ProgramProcessSubjectID(ProgramProcessSubject{Kind: "program_read", ProgramRead: &ref})
	other, _ := ProgramProcessSubjectID(ProgramProcessSubject{Kind: "program_read", ProgramRead: &changed})
	if id == other {
		t.Fatal("request evidence omitted from subject identity")
	}
}

func TestProgramProcessClosedReaderRejectsLiveGroupAndAcceptsActualGone(t *testing.T) {
	brain := canonicalProcessBrain(t)
	identity, _ := ownSleep(t)
	r := closedProcessForTest(processRecordForTest(ProgramProcessSubject{Kind: "normal_effect", EffectSlotID: strings.Repeat("1", 64)}, "execute"), identity)
	ref := writeProcessForTest(t, brain, r)
	if _, err := ReadClosedProgramProcess(context.Background(), brain, ref); err == nil {
		t.Fatal("caller closed flag hid a live writer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = StopOwnedProcessGroup(ctx, identity)
	until := time.Now().Add(2 * time.Second)
	for VerifyOwnedProcessGroupGone(identity) != nil && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	actual, err := ReadClosedProgramProcess(ctx, brain, ref)
	if err != nil || actual.Host != identity {
		t.Fatalf("actual group proof refused: %+v %v", actual.Host, err)
	}
}

func TestProgramProcessClosedReaderBindsWholeImmutableClosure(t *testing.T) {
	normal := ProgramProcessSubject{Kind: "normal_effect", EffectSlotID: strings.Repeat("1", 64)}
	original := closedProcessForTest(processRecordForTest(normal, "execute"), ProcessIdentity{})
	for _, change := range []string{"exact", "seal", "selection", "subject", "deadline", "nonce", "stage", "mode", "host", "revision"} {
		t.Run(change, func(t *testing.T) {
			r := original
			proof := *original.ClosedProof
			r.ClosedProof = &proof
			switch change {
			case "seal":
				r.ClosedProof.SealedRevision++
			case "selection":
				r.SelectionRevision++
			case "subject":
				r.ClosedProof.SubjectID = strings.Repeat("2", 64)
			case "deadline":
				r.Deadline = r.Deadline.Add(time.Second)
			case "nonce":
				r.NonceSHA256 = processDigest("changed-nonce")
			case "stage":
				r.Stage = "sealed"
			case "mode":
				r.Mode = "fetch"
			case "host":
				r.Host = ProcessIdentity{PID: 2, Group: 2, Birth: "invented"}
			case "revision":
				r.Revision++
			}
			brain := canonicalProcessBrain(t)
			ref := writeProcessForTest(t, brain, r)
			_, err := ReadClosedProgramProcess(context.Background(), brain, ref)
			if change == "exact" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatalf("changed %s certified", change)
			}
		})
	}
}

func TestProgramProcessBoundsAndCanonicalPath(t *testing.T) {
	read := ProgramProcessSubject{Kind: "program_read", ProgramRead: &ProgramReadRef{RequestID: strings.Repeat("2", 64), SHA256: processDigest("read")}}
	r := processRecordForTest(read, "read")
	r.Deadline = r.AdmittedAt.Add(16 * time.Second)
	r.EnrollmentSHA256 = ProgramProcessEnrollmentDigest(r)
	r.SelectionSHA256 = ProgramProcessDigest(ProgramProcessSelectionFor(r))
	if ValidateProgramProcessRecord(r) == nil {
		t.Fatal("read bound enlarged")
	}
	if n, _ := ProgramProcessLimit(read, "fetch"); n != 30*time.Minute {
		t.Fatal("existing fetch bound changed")
	}
	if _, err := ProgramProcessPath("/brain", strings.Repeat("1", 64), "../other"); err == nil {
		t.Fatal("claim path escaped")
	}
	root := canonicalProcessBrain(t)
	closed := closedProcessForTest(processRecordForTest(ProgramProcessSubject{Kind: "normal_effect", EffectSlotID: strings.Repeat("1", 64)}, "execute"), ProcessIdentity{})
	ref := writeProcessForTest(t, root, closed)
	p, _ := ProgramProcessPath(root, ref.SubjectID, ref.ClaimID)
	original, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadClosedProgramProcess(context.Background(), root, ref); err == nil {
		t.Fatal("public process evidence accepted")
	}
	if err := os.Chmod(p, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, append(original, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	ref.SHA256 = sha256.Sum256(append(original, '\n'))
	if _, err := ReadClosedProgramProcess(context.Background(), root, ref); err == nil {
		t.Fatal("noncanonical process body accepted")
	}
	if _, err := hex.DecodeString(ref.SHA256.String()); err != nil {
		t.Fatal(err)
	}
}

func TestProgramProcessRetryMetadataNeverExtendsAdmission(t *testing.T) {
	r := processRecordForTest(ProgramProcessSubject{Kind: "normal_effect", EffectSlotID: strings.Repeat("1", 64)}, "execute")
	r.Stage = "sealed"
	r.Revision = 2
	r.SealedRevision = 2
	originalDeadline, originalEnrollment, originalRevision := r.Deadline, r.EnrollmentSHA256, r.Revision
	oldHash := ProgramProcessDigest(r)
	r.NextCheckAt = r.Deadline.Add(time.Minute)
	if err := ValidateProgramProcessRecord(r); err != nil {
		t.Fatal(err)
	}
	if r.EnrollmentSHA256 != ProgramProcessEnrollmentDigest(r) || r.EnrollmentSHA256 != originalEnrollment || r.Revision != originalRevision || !r.Deadline.Equal(originalDeadline) {
		t.Fatal("retry changed admitted authority/deadline")
	}
	if oldHash == ProgramProcessDigest(r) {
		t.Fatal("retry metadata reused an old full-record hash")
	}
	r.Stage = "prepared"
	r.Revision = 1
	r.SealedRevision = 0
	if ValidateProgramProcessRecord(r) == nil {
		t.Fatal("prepared process accepted retry metadata")
	}
	r = closedProcessForTest(processRecordForTest(r.Subject, "execute"), ProcessIdentity{})
	r.NextCheckAt = r.Deadline.Add(time.Minute)
	if ValidateProgramProcessRecord(r) == nil {
		t.Fatal("closed process retained retry metadata")
	}
}

func TestRegistryChecksPhysicalModeHasOnlyProgrammeTwoMinuteBound(t *testing.T) {
	ref := ProgramReadRef{RequestID: strings.Repeat("1", 64), SHA256: processDigest("request")}
	cases := []struct {
		kind string
		ok   bool
	}{{"program_read", true}, {"normal_effect", false}, {"acceptance_read", false}}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			subject := ProgramProcessSubject{Kind: tc.kind}
			switch tc.kind {
			case "program_read":
				subject.ProgramRead = &ref
			case "normal_effect":
				subject.EffectSlotID = strings.Repeat("2", 64)
			case "acceptance_read":
				subject.AcceptanceRead = &AcceptanceReadGrantRef{Binding: strings.Repeat("3", 64), ProofID: "proof", SHA256: processDigest("grant")}
			}
			limit, err := ProgramProcessLimit(subject, "check")
			if tc.ok {
				if err != nil || limit != 2*time.Minute {
					t.Fatalf("programme check lifetime: %s %v", limit, err)
				}
			} else if err == nil {
				t.Fatal("unrelated subject acquired check execution lifetime")
			}
		})
	}
}
