package blueprint

// Guards is the registry of guard names a transition may use, each with the
// condition it checks. The validator refuses any other name; the program
// implements one check per name.
var Guards = map[string]string{
	// work
	"startable":            "the item's gate: start links are released, its repo's base branch is green and the account is ok",
	"step_started":         "the step's start event is logged",
	"step_ended":           "the step's end event is logged, and its output file has the hash it names and the step's headings",
	"verify_passed":        "the verifier's newest verify.v<n>.md is ended (as step_ended) and the first line under its ## Verdict is pass",
	"verify_failed":        "the verifier's newest verify.v<n>.md is ended (as step_ended) and the first line under its ## Verdict is fail",
	"question_asked":       "the session logged a question for the user",
	"answered":             "the user's answer is logged",
	"restarts_exhausted":   "the entity used up its restarts or nudges",
	"instruction_received": "the planner sent the entity an instruction event",
	"pr_recorded":          "a PR on factory/<work-id>, or the adopted PR, is recorded and the worktree is handed over",
	"merge_ready":          "required checks green on the head SHA, no unresolved thread, no CHANGES_REQUESTED, merge_quiet passed since the program first saw the PR's current head SHA, may-merge is 0, and an approval or `Merge without approval: yes`",
	"merge_failed":         "the merge call failed",
	"scope_changed":        "a re-check result says updated after the worktree was handed over",
	"cleaned_up":           "the item's worktrees and local branch are removed",
	"pr_merged":            "GitHub shows the PR merged",
	"pr_closed_unmerged":   "GitHub shows the PR closed without merging",
	"cancel_requested":     "a factory stop request covers the entity",
	"recheck_started":      "the epic started a re-check that covers the item",
	"recheck_kept":         "the re-check result for the item is ready or updated",
	"recheck_dropped":      "the re-check result for the item is done, obsolete or duplicate",
	"recheck_needs_user":   "the re-check result for the item waits on the user or on someone else",
	"needs_user":           "only the user can settle it: a dead-letter, planner runs used up, or the same SHA red after a babysitter pass",
	"retry_requested":      "the user ran factory retry on it",
	"retry_allowed":        "the user ran factory retry on it, and its PR is not closed",
	// set
	"lease_taken":       "an orchestrator holds the set's lease",
	"session_started":   "the entity's session is listed",
	"nothing_startable": "no open item can start now",
	"item_startable":    "an open item can start now",
	"items_finished":    "every item is merged or cancelled",
	// epic
	"state_check_ended": "the newest state-check.v<n>.md has its end event, and the epic has no state to return to: the first check, not a re-check",
	"epic_plan_ended":   "the newest epic.v<n>.yaml has its end event, is valid, and every issue has a result",
	"work_started":      "a set of the epic is claimed",
	"epic_idle":         "no open item of the epic can start, and none is being worked or in review",
	"all_in_review":     "every open item is in_review or later",
	"uat_passed":        "the epic has uat: none, or uat/result.md says pass",
	"recheck_signal":    "a re-check signal: issue_stale, a set blocked past its restarts, a related non-factory merge, factory replan, or factory answer",
	"recheck_ended":     "the re-check's newest epic.v<n>.yaml has its end event",
	// session
	"listed_working":      "claude agents lists the session with a pid, working",
	"turn_ended":          "claude agents lists the session with a pid, its turn ended",
	"checkpoint_ready":    "the session's newest event is checkpoint, its turn ended, and none of its steps has a start without an end",
	"compaction_recorded": "the transcript has a compaction record newer than the stop, and the turn ended",
	"factory_stopped":     "the factory stopped the session",
	"no_pid":              "the session has had no pid for supervision.dead_after",
	"resume_allowed":      "the account is ok and the session's work needs it again",
	"work_finished":       "the session's work is over and nothing will wake it",
	// account
	"usage_near_limit":  "the newest figure is fresh and at or above usage.near_limit in either window",
	"usage_under_limit": "the newest figure is fresh and under usage.near_limit in both windows",
	"usage_pause":       "a figure at or above usage.pause in either window, or a factory session shows a limit message",
	"reset_passed":      "usage.resume_after_reset has passed since the reset and the probe's fresh figure is under usage.near_limit",
	"all_resumed":       "every session stopped for the pause is resumed",
	"usage_stale":       "the newest figure is older than usage.stale_after",
}
