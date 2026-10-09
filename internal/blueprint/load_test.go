package blueprint

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

type fakeDM struct {
	texts []string
	err   error
}

func (f *fakeDM) DM(_ context.Context, text string) error {
	if f.err != nil {
		return f.err
	}
	f.texts = append(f.texts, text)
	return nil
}

func readString(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestCurrentSavesTheGoodCopy(t *testing.T) {
	brain := testBrain(t, testAgents)
	dm := &fakeDM{}
	b, problems, err := Current(context.Background(), brain, nil, dm)
	if err != nil || len(problems) != 0 || b == nil {
		t.Fatalf("Current = %v, %v, %v; want the blueprint and no problems", b, problems, err)
	}
	if readString(t, GoodPath(brain)) != readString(t, Path(brain)) {
		t.Error("the last good copy differs from the blueprint")
	}
	if len(dm.texts) != 0 {
		t.Errorf("DMs = %q, want none", dm.texts)
	}
}

func TestCurrentFallsBackAndDMsOncePerFailingVersion(t *testing.T) {
	brain := testBrain(t, testAgents)
	good := readString(t, Path(brain))
	dm := &fakeDM{}
	current := func() (*Blueprint, []Problem) {
		t.Helper()
		b, problems, err := Current(context.Background(), brain, nil, dm)
		if err != nil {
			t.Fatalf("Current: %v", err)
		}
		return b, problems
	}
	current()

	typo := strings.Replace(good, "schema_version: 1", "schema_version: 1\nschema_versoin: 1", 1)
	put(t, Path(brain), typo)
	b, problems := current()
	if b == nil || b.SchemaVersion != 1 || len(b.Machines) != 5 {
		t.Fatalf("blueprint = %+v, want the last good copy", b)
	}
	if len(problems) != 1 || problems[0].Rule != "yaml" || !strings.Contains(problems[0].Message, "field schema_versoin not found") {
		t.Fatalf("problems = %v, want the YAML typo", problems)
	}
	if len(dm.texts) != 1 || !strings.Contains(dm.texts[0], "The tick runs on the last good copy.") {
		t.Fatalf("DMs = %q, want one about the last good copy", dm.texts)
	}

	current()
	if len(dm.texts) != 1 {
		t.Fatalf("DMs after loading the same broken file again = %d, want 1", len(dm.texts))
	}

	put(t, Path(brain), strings.Replace(good, "schema_version: 1", "schema_version: 2", 1))
	if _, problems := current(); len(problems) != 1 || problems[0].Rule != "schema" {
		t.Fatalf("problems = %v, want the schema version", problems)
	}
	if len(dm.texts) != 2 {
		t.Fatalf("DMs after a second failing version = %d, want 2", len(dm.texts))
	}

	put(t, Path(brain), good)
	if _, problems := current(); len(problems) != 0 {
		t.Fatalf("problems = %v, want none once fixed", problems)
	}
	put(t, Path(brain), typo)
	current()
	if len(dm.texts) != 3 {
		t.Fatalf("DMs after the typo came back = %d, want 3", len(dm.texts))
	}
}

func TestCurrentWithoutAGoodCopy(t *testing.T) {
	brain := testBrain(t, testAgents)
	put(t, Path(brain), "schema_version: [\n")
	dm := &fakeDM{}
	for range 2 {
		b, problems, err := Current(context.Background(), brain, nil, dm)
		if b != nil || err == nil || len(problems) != 1 || problems[0].Rule != "yaml" {
			t.Fatalf("Current = %v, %v, %v; want no blueprint, the YAML problem and an error", b, problems, err)
		}
	}
	if len(dm.texts) != 1 || !strings.Contains(dm.texts[0], "There is no usable last good copy") {
		t.Fatalf("DMs = %q, want one saying there is no good copy", dm.texts)
	}
}

func TestCurrentRetriesAFailedDM(t *testing.T) {
	brain := testBrain(t, testAgents)
	Current(context.Background(), brain, nil, &fakeDM{})
	put(t, Path(brain), "schema_version: [\n")

	_, problems, _ := Current(context.Background(), brain, nil, &fakeDM{err: errors.New("slack is down")})
	if last := problems[len(problems)-1]; last != (Problem{Rule: "dm", Message: "slack is down"}) {
		t.Fatalf("problems = %v, want the DM failure last", problems)
	}
	dm := &fakeDM{}
	Current(context.Background(), brain, nil, dm)
	if len(dm.texts) != 1 {
		t.Fatalf("DMs after a failed one = %d, want 1", len(dm.texts))
	}
}
