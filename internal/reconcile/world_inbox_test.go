package reconcile

import (
	"context"
	"slices"
	"testing"
)

func TestWorldInboxDrainsCompletedPostsWithoutChangingSnapshots(t *testing.T) {
	w := newWorld(t)
	const pid = 4291
	posted := w.listen(pid)
	if got := posted(); len(got) != 0 {
		t.Fatalf("empty inbox = %q", got)
	}
	client := w.deps().Claude
	post := func(text string) {
		t.Helper()
		if err := client.Post(context.Background(), pid, text); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"first <&>\nnext line", "second"}
	for _, text := range want {
		post(text)
	}
	first := posted()
	if !slices.Equal(first, want) {
		t.Fatalf("queued posts = %q, want %q", first, want)
	}
	first[0] = "changed snapshot"
	repeated := posted()
	if !slices.Equal(repeated, want) {
		t.Fatalf("repeated snapshot = %q, want %q", repeated, want)
	}
	post("third")
	if got := posted(); !slices.Equal(got, []string{want[0], want[1], "third"}) {
		t.Fatalf("later post = %q", got)
	}
	if !slices.Equal(repeated, want) {
		t.Fatalf("later post changed earlier snapshot: %q", repeated)
	}
}
