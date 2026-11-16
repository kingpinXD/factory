package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/kingpinXD/factory/internal/store"
)

func TestDigestAgeLegacyDoesNotSubtractAccountPause(t *testing.T) {
	for _, mode := range []string{"claude"} {
		t.Run(mode, func(t *testing.T) {
			w := threeWaiting(t)
			w.at(25 * time.Hour)
			r, err := newRun(context.Background(), w.deps(), false)
			if err != nil {
				t.Fatal(err)
			}
			if err = r.loadBlueprint(); err != nil {
				t.Fatal(err)
			}
			r.attachMachines()
			it := r.ents["e1-w1"]
			r.sup.holds = []store.Hold{{From: it.cur.Since, To: r.now, State: statePaused}}
			if r.managedClock(it) {
				t.Fatal("legacy fixture used managed clock")
			}
			if got, want := r.digestAge(it), r.now.Sub(it.cur.Since); got != want || got != 25*time.Hour {
				t.Fatalf("legacy %s digest age=%v want%v", mode, got, want)
			}
			if r.legacyElapsed(it, it.cur.Since) != 0 {
				t.Fatal("fixture did not contain an account pause")
			}
		})
	}
}
