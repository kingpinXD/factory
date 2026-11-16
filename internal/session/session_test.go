package session

import "testing"

func TestFindByNamePreservesLiveThenNewestPreference(t *testing.T) {
	for _, test := range []struct {
		name     string
		sessions []Session
		want     string
	}{
		{"live before newer stopped", []Session{{Ref: Ref{ID: "live", Name: "worker", PID: 7}, StartedAt: 1}, {Ref: Ref{ID: "stopped", Name: "worker"}, StartedAt: 2}}, "live"},
		{"newer live before older live", []Session{{Ref: Ref{ID: "old", Name: "worker", PID: 7}, StartedAt: 1}, {Ref: Ref{ID: "new", Name: "worker", PID: 8}, StartedAt: 2}}, "new"},
		{"tie keeps first", []Session{{Ref: Ref{ID: "first", Name: "worker", PID: 7}, StartedAt: 1}, {Ref: Ref{ID: "second", Name: "worker", PID: 8}, StartedAt: 1}}, "first"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := FindByName(test.sessions, "worker")
			if !ok || got.ID != test.want {
				t.Fatalf("got %s, %v; want %s", got.ID, ok, test.want)
			}
		})
	}
}
