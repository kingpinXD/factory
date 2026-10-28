package agents

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/kingpinXD/factory/internal/helper"
)

func TestNativeSessionHelperRequestSchemas(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		t.Run(name, func(t *testing.T) {
			prompt := read(t, PromptPathFor(out, name))
			example := func(heading string, keys []string, dst any) {
				t.Helper()
				match := regexp.MustCompile(regexp.QuoteMeta(heading) + "\\s+~~~json\\n([^~]+)\\n~~~").FindStringSubmatch(prompt)
				if len(match) != 2 {
					t.Fatalf("generated prompt missing complete %s JSON example", heading)
				}
				var fields map[string]json.RawMessage
				if err := json.Unmarshal([]byte(match[1]), &fields); err != nil {
					t.Fatal(err)
				}
				got := make([]string, 0, len(fields))
				for key := range fields {
					got = append(got, key)
				}
				sort.Strings(got)
				sort.Strings(keys)
				if !reflect.DeepEqual(got, keys) {
					t.Fatalf("%s schema fields: got %v, want %v", heading, got, keys)
				}
				dec := json.NewDecoder(strings.NewReader(match[1]))
				dec.DisallowUnknownFields()
				if err := dec.Decode(dst); err != nil {
					t.Fatal(err)
				}
			}
			var request helper.Request
			example("Submit Request (request.json):", []string{"schema_version", "request_key", "owner_id", "work_id", "set_id", "lease", "parent_job_id", "component", "prompt", "output_path"}, &request)
			want := helper.Request{SchemaVersion: 1, RequestKey: "review-round-1", OwnerID: "OWNER_ID", WorkID: "WORK_ID", SetID: "SET_ID", Lease: 1, ParentJobID: "", Component: "plan-reviewer", Prompt: "Review the supplied plan; return findings in your reply.", OutputPath: ""}
			if request != want {
				t.Fatalf("submit example: got %+v, want %+v", request, want)
			}
			var continuation helper.ContinueRequest
			example("ContinueRequest (continue.json):", []string{"schema_version", "request_key", "expected_round", "prompt", "output_path", "acknowledged_children"}, &continuation)
			next := helper.ContinueRequest{SchemaVersion: 1, RequestKey: "review-round-2", ExpectedRound: 1, Prompt: "Review the revised plan in the same conversation.", OutputPath: "", AcknowledgedChildren: []string{}}
			if !reflect.DeepEqual(continuation, next) {
				t.Fatalf("continue example: got %+v, want %+v", continuation, next)
			}
			for _, required := range []string{
				`"$FACTORY_BIN" helper submit --brain "$FACTORY_BRAIN" --request request.json`,
				`"$FACTORY_BIN" helper continue --brain "$FACTORY_BRAIN" --job "$JOB_ID" --request continue.json`,
				`"$FACTORY_BIN" helper result --brain "$FACTORY_BRAIN" --job "$JOB_ID" --round 1`,
				`"$FACTORY_BIN" helper yield --brain "$FACTORY_BRAIN" --job "$JOB_ID" --request yield.json`,
				`{"children":["CHILD_JOB_ID"]}`,
				"Copy owner_id, work_id, set_id and lease from the current factory instructions",
				"Never guess IDs or lease values", "Use lease 0 when set_id is empty",
				"parent_job_id is empty for a top-level dispatch",
				"letters, digits, hyphen, underscore or dot", "not a timestamp regenerated on retry",
				"After a lost acknowledgment, resend the identical JSON with the same request_key",
				"expected_round is the job's current stopped round, not the next round",
				"acknowledged_children must name exactly the failed, stopped children",
				`For reply-only work, output_path is ""`,
				"The manager owns helper start/end through the runner",
				"Never fall back to an external Claude Agent",
			} {
				if !strings.Contains(prompt, required) {
					t.Errorf("generated prompt missing %q", required)
				}
			}
		})
	}
}

func TestNativeSessionTransportSnapshotRetained(t *testing.T) {
	brain, b, _, out := nativeBrain(t)
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(out, "sources.json")
	snapshot, err := readSources(path)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.PromptProtocol != 1 {
		t.Fatal("expected current transport protocol")
	}
	snapshot.SessionTransport = "historical session transport without request schemas"
	// Seed a separate selection with historical sources before any prompts exist.
	brain, b, _, out = nativeBrain(t)
	path = filepath.Join(out, "sources.json")
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	put(t, path, string(data))
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	before := map[string]string{}
	for _, name := range []string{"planner", "orchestrator", "babysitter"} {
		prompt := read(t, PromptPathFor(out, name))
		if !strings.Contains(prompt, snapshot.SessionTransport) || strings.Contains(prompt, "Submit Request (request.json):") {
			t.Fatal("historical session transport replaced")
		}
		before[name] = prompt
	}
	if _, _, err := BuildFor(brain, out, b, nil); err != nil {
		t.Fatal(err)
	}
	if read(t, path) != string(data) {
		t.Fatal("saved source snapshot changed")
	}
	for name, prompt := range before {
		if read(t, PromptPathFor(out, name)) != prompt {
			t.Fatalf("%s saved prompt changed", name)
		}
	}
}
