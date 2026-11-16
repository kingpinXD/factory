package helper

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExistingPRModeRejectsCorruptBindings(t *testing.T) {
	request := Request{SchemaVersion: 1, RequestKey: "repair", OwnerID: "work1", WorkID: "work1", Component: "implementor", Prompt: "fix", Mode: ModeExistingPRRevision, RevisionPR: 7, RevisionRepo: "o/r", RevisionBranch: "fix/owned", RevisionBase: "main"}
	job := Job{Request: request, Scope: Scope{OwnerID: "work1", WorkID: "work1"}}
	job.Route.Provider = "codex"
	if err := validateExistingPRMode(job); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Job){func(j *Job) { j.Request.RevisionPR = 0 }, func(j *Job) { j.Request.RevisionRepo = "other" }, func(j *Job) { j.Request.RevisionBranch = "" }, func(j *Job) { j.Route.Provider = "zed" }, func(j *Job) { j.Request.Component = "verifier" }, func(j *Job) { j.Request.Mode = "unknown" }, func(j *Job) { j.Request.OwnerID = "other" }} {
		bad := job
		mutate(&bad)
		if err := validateExistingPRMode(bad); err == nil {
			t.Fatalf("corrupt mode accepted: %+v", bad)
		}
	}
}
func TestExistingPRModeZeroPreservesLegacyJSON(t *testing.T) {
	raw, err := json.Marshal(Request{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"mode"`) {
		t.Fatal("zero mode changed request JSON")
	}
	raw, err = json.Marshal(Scope{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"revision_`) {
		t.Fatal("zero revision changed legacy scope JSON")
	}
}
