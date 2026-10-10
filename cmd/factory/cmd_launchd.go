package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kingpinXD/factory/internal/blueprint"
	"github.com/kingpinXD/factory/internal/proc"
	"github.com/kingpinXD/factory/internal/store"
)

// launchJob is one LaunchAgent: its label, and the factory command it runs
// every minute.
type launchJob struct{ label, command string }

var launchJobs = []launchJob{
	{"com.tanmay.factory-tick", "tick"},
	{"com.tanmay.factory-repo-worker", "repo-worker"},
}

// launchctl runs launchctl. Tests replace it.
var launchctl proc.Runner = proc.Exec{}

// launchPATH follows ~/.local/bin, where `make install` puts factory and
// where claude lives: Homebrew's gh and git, then the system's.
const launchPATH = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

// launchd places the jobs for one user: their plists, the binary they run,
// the brain they act on and their logs.
type launchd struct {
	home, brain string
	uid         int
}

func (l launchd) plistPath(j launchJob) string {
	return filepath.Join(l.home, "Library", "LaunchAgents", j.label+".plist")
}

func (l launchd) logPath(j launchJob) string {
	return filepath.Join(l.home, ".factory", "logs", j.command+".log")
}

func (l launchd) service(j launchJob) string { return "gui/" + strconv.Itoa(l.uid) + "/" + j.label }

// plist is the job's LaunchAgent. It carries no token: the program reads
// GH_TOKEN from the brain's secrets when launchd starts it without one.
func (l launchd) plist(j launchJob) []byte {
	esc := func(s string) string {
		var e strings.Builder
		xml.EscapeText(&e, []byte(s))
		return e.String()
	}
	return fmt.Appendf(nil, `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<!-- Written by factory launchd install; factory launchd remove deletes it. -->
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>%s</string>
	<key>ProgramArguments</key>
	<array>
		<string>%s</string>
		<string>%s</string>
	</array>
	<key>EnvironmentVariables</key>
	<dict>
		<key>FACTORY_BRAIN</key>
		<string>%s</string>
		<key>PATH</key>
		<string>%s</string>
	</dict>
	<key>RunAtLoad</key>
	<false/>
	<key>StartInterval</key>
	<integer>60</integer>
	<key>StandardOutPath</key>
	<string>%s</string>
	<key>StandardErrorPath</key>
	<string>%s</string>
</dict>
</plist>
`, esc(j.label), esc(filepath.Join(l.home, ".local", "bin", "factory")), esc(j.command), esc(l.brain),
		esc(filepath.Join(l.home, ".local", "bin")+":"+launchPATH), esc(l.logPath(j)), esc(l.logPath(j)))
}

// install writes each job's plist and loads it, booting out a loaded copy
// first, so running it again leaves the same two jobs.
func (l launchd) install(ctx context.Context) error {
	for _, j := range launchJobs {
		if err := l.bootout(ctx, j); err != nil {
			return err
		}
		for _, dir := range []string{filepath.Dir(l.plistPath(j)), filepath.Dir(l.logPath(j))} {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
		}
		if err := os.WriteFile(l.plistPath(j), l.plist(j), 0o644); err != nil {
			return err
		}
		if _, err := launchctl.Run(ctx, proc.Cmd{Name: "launchctl", Args: []string{"bootstrap", "gui/" + strconv.Itoa(l.uid), l.plistPath(j)}}); err != nil {
			return err
		}
	}
	return nil
}

// remove boots out and deletes both jobs, then deletes last_tick, so no
// session warns that the stopped factory's tick is late.
func (l launchd) remove(ctx context.Context) error {
	for _, j := range launchJobs {
		if err := l.bootout(ctx, j); err != nil {
			return err
		}
		if err := os.Remove(l.plistPath(j)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	o, err := store.ReadOverall(l.brain)
	if err != nil || o.LastTick.IsZero() {
		return err
	}
	o.LastTick = time.Time{}
	return store.WriteOverall(l.brain, o)
}

// bootout unloads the job when launchctl lists it.
func (l launchd) bootout(ctx context.Context, j launchJob) error {
	if _, err := launchctl.Run(ctx, proc.Cmd{Name: "launchctl", Args: []string{"print", l.service(j)}}); err != nil {
		return nil
	}
	_, err := launchctl.Run(ctx, proc.Cmd{Name: "launchctl", Args: []string{"bootout", l.service(j)}})
	return err
}

// runLaunchd installs or removes the jobs that run the factory every
// minute: factory launchd install|remove.
func runLaunchd(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 || args[0] != "install" && args[0] != "remove" {
		fmt.Fprintln(stderr, "usage: factory launchd install|remove")
		return 2
	}
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Fprintf(stderr, "factory launchd: %v\n", err)
		return 1
	}
	l := launchd{home: home, brain: blueprint.Brain(), uid: os.Getuid()}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	do, did := l.install, "installed"
	if args[0] == "remove" {
		do, did = l.remove, "removed"
	}
	if err := do(ctx); err != nil {
		fmt.Fprintf(stderr, "factory launchd %s: %v\n", args[0], err)
		return 1
	}
	for _, j := range launchJobs {
		fmt.Fprintf(stdout, "%s %s (%s)\n", did, j.label, l.plistPath(j))
	}
	return 0
}
