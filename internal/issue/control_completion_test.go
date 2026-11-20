package issue

import (
	"context"
	"testing"

	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/toolcontract"
)

func TestControlCompletionBindsParentAndAllCompletedSteps(t *testing.T) {
	h := newHub()
	h.add(1, "epic", "open")
	w := writer(t, h)
	input := toolcontract.IssueInput{Repo: "o/r", Key: "child", Title: "Child", Body: "Task", Parent: "o/r#1"}
	var proof []events.Event
	w.Completed = func(log []events.Event) error {
		var err error
		proof, err = CompletionEvidence("issue_create", input, log)
		return err
	}
	if _, err := w.Create(context.Background(), input.Repo, input.Key, input.Title, input.Body, input.Parent); err != nil {
		t.Fatal(err)
	}
	if len(proof) != 8 {
		t.Fatalf("create and attachment need request/intent/proof/done each, got %d", len(proof))
	}
	if err := ValidateControlCompletion("issue_create", input, proof); err != nil {
		t.Fatal(err)
	}
	changed := input
	changed.Parent = "o/r#2"
	if err := ValidateControlCompletion("issue_create", changed, proof); err == nil {
		t.Fatal("completion accepted a different parent")
	}
	if err := ValidateControlCompletion("issue_create", input, proof[:4]); err == nil {
		t.Fatal("creation alone proved parent attachment")
	}
	changed = input
	changed.Body = "Other task"
	if err := ValidateControlCompletion("issue_create", changed, proof); err == nil {
		t.Fatal("completion accepted different body")
	}
	t.Log("proof binds original issue body, parent, and both completed steps")
}
