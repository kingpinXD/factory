package gh

import (
	"errors"
	"reflect"
	"testing"
)

// The replies are GitHub's, recorded on 2026-10-10 for zeta-chain/argo-cd-apps
// env-dev (a merge queue) and kingpinXD/factory main (none).
func TestMergeMethodFor(t *testing.T) {
	for _, tc := range []struct {
		name, reply string
		want        MergeMethod
	}{
		{"merge queue", `{"data":{"repository":{"mergeQueue":{"id":"MQ_kwDOL34IXs4AAUno"}}}}`, MergeQueue},
		{"no merge queue", `{"data":{"repository":{"mergeQueue":null}}}`, MergeSquash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fakeGH(map[string]reply{"mergeQueue(": {out: tc.reply}})
			got, err := (Client{Runner: f}).MergeMethodFor(ctx, "zeta-chain/argo-cd-apps", "env-dev")
			if err != nil || got != tc.want {
				t.Fatalf("MergeMethodFor = %q, %v; want %q", got, err, tc.want)
			}
			want := []string{"gh", "api", "graphql", "-f", "query=" + mergeQueueQuery,
				"-f", "owner=zeta-chain", "-f", "name=argo-cd-apps", "-f", "branch=env-dev"}
			if calls := args(f); len(calls) != 1 || !reflect.DeepEqual(calls[0], want) {
				t.Errorf("calls = %q", calls)
			}
		})
	}
	f := fakeGH(map[string]reply{"mergeQueue(": {err: errors.New("HTTP 502")}})
	if _, err := (Client{Runner: f}).MergeMethodFor(ctx, "o/r", "main"); err == nil {
		t.Error("a failed lookup returned no error")
	}
}
