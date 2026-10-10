package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/epic"
	"github.com/kingpinXD/factory/internal/events"
	"github.com/kingpinXD/factory/internal/reconcile"
	"github.com/kingpinXD/factory/internal/store"
)

// agentKinds are the kinds `factory event` writes. Every other kind is
// written by the program itself.
var agentKinds = []string{
	events.KindStart, events.KindEnd, events.KindError, events.KindIntent, events.KindDone,
	events.KindIssueStale, events.KindInstruction, events.KindMessage, events.KindCheckpoint,
}

// contextLine opens every inbox and heartbeat reply: the context use of the
// session working for id.
func contextLine(id string) string { return reconcile.ContextLine(blueprint.Brain(), id) }

// sender is the id of the factory session running the command, or "user".
func sender() string {
	if id := os.Getenv("FACTORY_TASK"); id != "" {
		return id
	}
	return "user"
}

// lookup resolves id to its entry in the brain's index.json.
func lookup(id string) (store.Entry, error) {
	ix, err := store.ReadIndex(blueprint.Brain())
	if err != nil {
		return store.Entry{}, err
	}
	return ix.Lookup(id)
}

// runEvent logs one event: factory event <id> <kind> [--file <path>]
// [--text "<one line>"] [--to user|<id>]. A message --to another id goes to
// that id's log, so it reaches that id's inbox.
func runEvent(args []string, stdout, stderr io.Writer) int {
	const usage = `usage: factory event <id> <kind> [--file <path>] [--text "<one line>"] [--to user|<id>]`
	if len(args) < 2 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	id, kind := args[0], args[1]
	fs := flag.NewFlagSet("event", flag.ContinueOnError)
	fs.SetOutput(stderr)
	file := fs.String("file", "", "the step's output file")
	text := fs.String("text", "", "one line")
	to := fs.String("to", "", "user, or the id a message is for")
	if err := fs.Parse(args[2:]); err != nil || fs.NArg() != 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	if err := checkEvent(kind, *file, *text, *to); err != nil {
		fmt.Fprintf(stderr, "factory event: %v\n", err)
		return 2
	}

	logged, err := logEvent(id, events.Event{Kind: kind, Sender: sender(), Text: *text}, *file, *to)
	if err != nil {
		fmt.Fprintf(stderr, "factory event: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "seq: %d\n", logged.Seq)
	return 0
}

// logEvent appends ev to id's log, with file's absolute path and hash.
func logEvent(id string, ev events.Event, file, to string) (events.Event, error) {
	if to == events.RecipientUser {
		ev.Recipient = to
	} else if to != "" {
		id = to
	}
	if file != "" {
		var err error
		if ev.File, err = filepath.Abs(file); err != nil {
			return events.Event{}, err
		}
		if ev.SHA256, err = events.HashFile(ev.File); err != nil {
			return events.Event{}, err
		}
	}
	return store.AppendTo(blueprint.Brain(), id, ev)
}

func checkEvent(kind, file, text, to string) error {
	switch {
	case !slices.Contains(events.Kinds, kind):
		return fmt.Errorf("unknown event kind: %s", kind)
	case !slices.Contains(agentKinds, kind):
		return fmt.Errorf("%s events are written by the program, not by factory event", kind)
	case (kind == events.KindEnd || kind == events.KindCheckpoint) && file == "":
		return fmt.Errorf("%s needs --file <path>", kind)
	case (kind == events.KindMessage || kind == events.KindInstruction) && text == "":
		return fmt.Errorf("%s needs --text", kind)
	case strings.ContainsAny(text, "\r\n"):
		return errors.New("--text must be one line")
	case to != "" && kind != events.KindMessage:
		return errors.New("--to applies only to message events")
	}
	return nil
}

// runInbox prints the unacknowledged messages for an id, or acks one:
// factory inbox <id> [--ack <seq>] [--read-only].
func runInbox(args []string, stdout, stderr io.Writer) int {
	const usage = "usage: factory inbox <id> [--ack <seq>] [--read-only]"
	if len(args) < 1 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	fs := flag.NewFlagSet("inbox", flag.ContinueOnError)
	fs.SetOutput(stderr)
	ack := fs.Int("ack", 0, "the seq to acknowledge")
	readOnly := fs.Bool("read-only", false, "never ack (helpers)")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	if *readOnly && *ack != 0 {
		fmt.Fprintln(stderr, "factory inbox: --read-only cannot ack; your session acks its messages")
		return 2
	}
	entry, err := lookup(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "factory inbox: %v\n", err)
		return 1
	}
	path := store.EventsPath(entry.Dir)
	fmt.Fprintln(stdout, contextLine(args[0]))
	if *ack != 0 {
		if err := events.Ack(path, *ack, sender()); err != nil {
			fmt.Fprintf(stderr, "factory inbox: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "acked: %d\n", *ack)
		return 0
	}
	evs, err := events.Read(path)
	if err != nil {
		fmt.Fprintf(stderr, "factory inbox: %v\n", err)
		return 1
	}
	for _, m := range events.Inbox(evs) {
		fmt.Fprintf(stdout, "\nseq: %d\nkind: %s\nfrom: %s\ntext: %s\n", m.Seq, m.Kind, m.Sender, m.Text)
	}
	return 0
}

// runHeartbeat renews a set's lease: factory heartbeat <set-id> <lease>.
func runHeartbeat(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		fmt.Fprintln(stdout, contextLine(args[0]))
	}
	dir, lease, code := checkLease("heartbeat", args, stderr)
	if code != 0 {
		return code
	}
	if _, err := events.Append(store.EventsPath(dir), events.Event{Kind: events.KindHeartbeat, Sender: sender(), Lease: lease}); err != nil {
		fmt.Fprintf(stderr, "factory heartbeat: %v\n", err)
		return 1
	}
	return 0
}

// runLeaseOK exits 0 while the lease is the set's current one:
// factory lease-ok <set-id> <lease>.
func runLeaseOK(args []string, _, stderr io.Writer) int {
	_, _, code := checkLease("lease-ok", args, stderr)
	return code
}

// checkLease returns the folder of the set args name and the lease they
// give, with exit code 0 when that lease is the set's current one, 1 when
// it is not or the set cannot be read, and 2 for bad arguments.
func checkLease(cmd string, args []string, stderr io.Writer) (string, int, int) {
	if len(args) != 2 {
		fmt.Fprintf(stderr, "usage: factory %s <set-id> <lease>\n", cmd)
		return "", 0, 2
	}
	lease, err := strconv.Atoi(args[1])
	if err != nil || lease < 1 {
		fmt.Fprintf(stderr, "factory %s: lease must be a number from 1: %q\n", cmd, args[1])
		return "", 0, 2
	}
	entry, err := lookup(args[0])
	if err == nil && entry.Kind != blueprint.MachineSet {
		err = fmt.Errorf("%s is a %s, not a set", args[0], entry.Kind)
	}
	var st store.Status
	if err == nil {
		st, err = store.ReadStatus(entry.Dir)
	}
	if err != nil {
		fmt.Fprintf(stderr, "factory %s: %v\n", cmd, err)
		return "", 0, 1
	}
	if st.Lease != lease {
		fmt.Fprintf(stderr, "factory %s: lease %d is not the current lease of %s (current: %d)\n", cmd, lease, args[0], st.Lease)
		return "", 0, 1
	}
	return entry.Dir, lease, 0
}

// item is one in-flight work item, as factory query items prints it.
type item struct {
	id, epic, state string
	issue           int
	// files lists the paths of the item's newest ended explore output;
	// filesErr says why they are unknown.
	files    []string
	filesErr error
}

// runQuery answers from the program's own files:
// factory query items --repo <owner/repo> [--files <path>...].
func runQuery(args []string, stdout, stderr io.Writer) int {
	repo, files, err := parseQuery(args)
	if err != nil {
		fmt.Fprintf(stderr, "factory query: %v\nusage: factory query items --repo <owner/repo> [--files <path>...]\n", err)
		return 2
	}
	ix, err := store.ReadIndex(blueprint.Brain())
	if err != nil {
		fmt.Fprintf(stderr, "factory query: %v\n", err)
		return 1
	}
	items, err := inFlight(ix, repo)
	if err != nil {
		fmt.Fprintf(stderr, "factory query: %v\n", err)
		return 1
	}
	printed := 0
	for _, it := range items {
		if len(files) > 0 && it.filesErr == nil && !epic.Overlap(it.files, files) {
			continue
		}
		listed := strings.Join(it.files, " ")
		if it.filesErr != nil {
			listed = "? (" + it.filesErr.Error() + ")"
		}
		if printed > 0 {
			fmt.Fprintln(stdout)
		}
		fmt.Fprintf(stdout, "id: %s\nepic: %s\nstate: %s\nissue: %d\nfiles: %s\n", it.id, it.epic, it.state, it.issue, listed)
		printed++
	}
	if printed == 0 {
		fmt.Fprintf(stdout, "no matching in-flight items in %s\n", repo)
	}
	return 0
}

func parseQuery(args []string) (repo string, files []string, err error) {
	if len(args) == 0 || args[0] != "items" {
		return "", nil, errors.New("only items can be queried")
	}
	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--repo":
			if i++; i == len(args) {
				return "", nil, errors.New("--repo needs owner/repo")
			}
			repo = args[i]
		case "--files":
			n := len(files)
			for i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				i++
				files = append(files, args[i])
			}
			if len(files) == n {
				return "", nil, errors.New("--files needs at least one path")
			}
		default:
			return "", nil, fmt.Errorf("unknown argument %q", args[i])
		}
	}
	if repo == "" {
		return "", nil, errors.New("--repo is required")
	}
	return repo, files, nil
}

// inFlight returns the work items in repo that are not in an end state,
// across every epic, sorted by id.
func inFlight(ix store.Index, repo string) ([]item, error) {
	var items []item
	for _, id := range slices.Sorted(maps.Keys(ix)) {
		e := ix[id]
		if e.Kind != blueprint.MachineWork {
			continue
		}
		st, err := store.ReadStatus(e.Dir)
		if err != nil {
			return nil, err
		}
		if st.End || !strings.EqualFold(st.Repo, repo) {
			continue
		}
		it := item{id: id, epic: e.Epic, state: st.State, issue: st.Issue}
		end, err := events.Output(store.EventsPath(e.Dir), "explore")
		if err == nil {
			it.files, err = epic.ExploreFiles(end.File)
		}
		if errors.Is(err, events.ErrNotEnded) {
			err = errors.New("not explored yet")
		}
		it.filesErr = err
		items = append(items, it)
	}
	return items, nil
}
