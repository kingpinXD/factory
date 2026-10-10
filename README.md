# factory

An always-running pipeline that takes an issue, epic or plan to merged PRs, using Claude Code sessions.

The plan and its decisions live in the author's agent harness, not in this repo.

```sh
make build      # go build ./...
make test       # go test -race ./...
make install    # builds ~/.local/bin/factory
```

## `factory status --json`

One JSON object. Times are UTC RFC 3339; a list is `[]` when empty. `factory status` without
`--json` prints the same report as text, with ages counted to `at`.

```jsonc
{
  "at": "2026-10-09T15:00:00Z",          // when the report was read
  "last_tick": "2026-10-09T14:59:00Z",   // left out when no tick ran, or after `factory launchd remove`
  "tick": 42,                            // the last finished tick's number
  "account": {                           // factory/account/status.json; {"state": ""} before the first tick
    "state": "near_limit",               // ok | near_limit | paused | resuming | stale
    "since": "…",
    "usage": {                           // the newest usage figure younger than usage.stale_after; left out when none
      "session_id": "…", "written_at": "…",
      "five_hour": {"used_percentage": 82, "resets_at": 1791561600},   // resets_at in Unix seconds
      "seven_day": {"used_percentage": 61, "resets_at": 1791817200}
    },
    "resets_at": "…", "weekly": true,    // the reset a pause waits for; left out when none
    "probed_at": "…", "errors": ["…"]    // left out when none
  },
  "sessions": {"running": 2, "by_repo": {"o/r": 2}},   // sessions not stopped, dead or finished; a planner's are in no repo
  "waiting_on_user": [                   // entities in needs_you or waiting_user
    {"id": "e1-w2", "state": "needs_you", "since": "…", "why": "the question asked, or why it needs you"}
  ],
  "in_review": [{"repo": "o/r", "count": 1, "oldest": "e1-w1", "oldest_since": "…"}],
  "dead_letters": [{"id": "e1-w2", "error": "the error reported too often"}],
  "paused_repos": ["o/r@main"],          // base branches whose checks were red: nothing new starts there
  "epics": [{"id": "e1", "state": "running", "since": "…", "rechecks": 1}],   // open epics
  "agents": [                            // sessions not finished
    {"name": "factory:e1-s1:orchestrator",   // for claude attach <name> or claude logs <name>
     "id": "a1b2c3d4",                   // the short id they also take; "" until a tick saw it listed
     "task": "e1-s1", "state": "running",
     "context": 54,                      // percent of values.context.window; null until measured
     "unread": 2, "oldest_unread": "…"}  // messages to the task not acknowledged; oldest_unread left out at 0
  ],
  "open_prs": [                          // work items whose PR is open
    {"id": "e1-w1", "url": "…", "state": "in_review",
     "unanswered": 2,                    // unresolved threads whose last comment is not the user's
     "merge_hold": "…"}                  // what kept it from merging at the last full reconcile; "" when nothing
  ],
  "errors": ["…"],                       // what the last tick went on past
  "problems": ["…"]                      // blueprint problems while the tick runs on the last good copy
}
```
