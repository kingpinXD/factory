package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/issue"
	"github.com/kingpinXD/factory/internal/store"
)

const issueUsage = `usage:
  factory issue create  --repo <owner/repo> --key <stable key> --title "<t>" --body-file <path> [--parent <number>]
  factory issue update  --repo <owner/repo> --number <n> --append-file <path>
  factory issue comment --repo <owner/repo> --number <n> --body-file <path>
  factory issue close   --repo <owner/repo> --number <n> --evidence <kind:ref> [--comment-file <path>]
  factory issue assign  --repo <owner/repo> --number <n>`

// runIssue makes one GitHub issue write for an agent, inside an intent and a
// done event in the log of the session running it (FACTORY_TASK).
func runIssue(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, issueUsage)
		return 2
	}
	fs := flag.NewFlagSet("issue "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	repo := fs.String("repo", "", "owner/repo")
	number := fs.Int("number", 0, "the issue's number")
	key := fs.String("key", "", "a stable key for the new issue's marker")
	title := fs.String("title", "", "the new issue's title")
	parent := fs.String("parent", "", "the issue the new one is a sub-issue of")
	bodyFile := fs.String("body-file", "", "the body or comment")
	appendFile := fs.String("append-file", "", "the section to append")
	evidence := fs.String("evidence", "", "pr:<url>, commit:<sha>, duplicate:<number> or children")
	commentFile := fs.String("comment-file", "", "the closing comment")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() != 0 || *repo == "" {
		fmt.Fprintln(stderr, issueUsage)
		return 2
	}
	var w issue.Writer
	ctx, cancel := context.WithTimeout(context.Background(), epicCommandTimeout)
	defer cancel()
	writes := map[string]func() (string, error){
		"create": func() (string, error) {
			body, err := readFlagFile("--body-file", *bodyFile, true)
			if err != nil {
				return "", err
			}
			return w.Create(ctx, *repo, *key, *title, body, *parent)
		},
		"update": func() (string, error) {
			section, err := readFlagFile("--append-file", *appendFile, true)
			if err != nil {
				return "", err
			}
			return "done", w.Update(ctx, *repo, *number, section)
		},
		"comment": func() (string, error) {
			body, err := readFlagFile("--body-file", *bodyFile, true)
			if err != nil {
				return "", err
			}
			return "done", w.Comment(ctx, *repo, *number, body)
		},
		"close": func() (string, error) {
			comment, err := readFlagFile("--comment-file", *commentFile, false)
			if err != nil {
				return "", err
			}
			return "done", w.Close(ctx, *repo, *number, *evidence, comment)
		},
		"assign": func() (string, error) { return "done", w.Assign(ctx, *repo, *number) },
	}
	write, ok := writes[args[0]]
	if !ok || args[0] != "create" && *number < 1 {
		fmt.Fprintln(stderr, issueUsage)
		return 2
	}
	w, err := issueWriter(stdout)
	if err != nil {
		fmt.Fprintf(stderr, "factory issue: %v\n", err)
		return 1
	}
	out, err := write()
	if err != nil {
		fmt.Fprintf(stderr, "factory issue %s: %v\n", args[0], err)
		return 1
	}
	fmt.Fprintln(stdout, out)
	return 0
}

// issueWriter writes for the session in FACTORY_TASK, logging in its log;
// for the user, it logs nothing.
func issueWriter(stdout io.Writer) (issue.Writer, error) {
	d, err := newDeps(stdout)
	if err != nil {
		return issue.Writer{}, err
	}
	w := issue.Writer{GitHub: d.GitHub, Brain: d.Brain, Sender: sender(), Now: time.Now}
	id := os.Getenv("FACTORY_TASK")
	if id == "" {
		return w, nil
	}
	entry, err := lookup(id)
	if err != nil {
		return issue.Writer{}, err
	}
	w.Log, w.Epic = store.EventsPath(entry.Dir), entry.Epic
	if entry.Kind == blueprint.MachineEpic {
		w.Epic = id
	}
	return w, nil
}

// readFlagFile reads the file a flag names; an empty name is an error when
// the flag is required.
func readFlagFile(flag, path string, required bool) (string, error) {
	if path == "" {
		if required {
			return "", fmt.Errorf("%s is required", flag)
		}
		return "", nil
	}
	data, err := os.ReadFile(path)
	return string(data), err
}
