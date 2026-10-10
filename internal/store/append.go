package store

import (
	"errors"
	"io/fs"
	"time"

	"github.com/kingpinXD/factory/internal/events"
)

// movedWait is how long AppendTo waits before it looks an entity up again.
const movedWait = 100 * time.Millisecond

// AppendTo logs ev in the event log of entity id, found through brain's
// index. When the entity's folder is gone mid-append, as while the tick
// moves an epic into features/, it looks the folder up again once: the move
// leaves a link at the old path, so the append lands in the new folder.
func AppendTo(brain, id string, ev events.Event) (events.Event, error) {
	for try := 0; ; try++ {
		ix, err := ReadIndex(brain)
		if err != nil {
			return events.Event{}, err
		}
		entry, err := ix.Lookup(id)
		if err != nil {
			return events.Event{}, err
		}
		logged, err := events.Append(EventsPath(entry.Dir), ev)
		if !errors.Is(err, fs.ErrNotExist) || try > 0 {
			return logged, err
		}
		time.Sleep(movedWait)
	}
}
