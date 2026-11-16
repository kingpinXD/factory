#!/usr/bin/env python3
"""Stage pinned revisions and, only with --run, compare real capture artifacts."""

import argparse
import base64
import contextlib
import copy
import datetime
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile

SHARED = ("internal/reconcile/claude_compat_test.go", "cmd/factory/claude_compat_test.go")
CANDIDATE = ("internal/reconcile/claude_compat_candidate_test.go",
             "cmd/factory/claude_compat_candidate_test.go")
# Only these four exact test paths may replace committed bytes. Fixtures never
# inherit this permission, even when explicitly listed as shared overlays.
REPLACEABLE_HARNESS = frozenset(SHARED + CANDIDATE)
# Extend only after the exact fixture inventory is agreed with the capture owner.
FIXTURES = ()
OWN_FILES = {"scripts/claude-compat.py", "scripts/test_claude_compat.py", "docs/claude-compat.md"}
LANES = ("main-default", "candidate-default", "candidate-explicit")
PACKAGES = {"reconcile": "internal/reconcile", "cli": "cmd/factory"}
INVENTORY = {
    "reconcile": {
        "epic_lifecycle": ("intake", "planned", "prepared", "phases", "review_merge", "cleanup"),
        "configuration": ("defaults", "overrides"),
        "usage_policy": ("hold", "pause", "reset", "stale"),
        "review_and_ci": ("checks_failed", "changes_requested", "reviews_resolved", "head_changed", "merge"),
        "restart": ("start", "reload", "unknown_listing", "resume"),
        "checkpoint": ("open_step", "checkpoint", "compact", "readback"),
        "legacy_state": ("before_restart", "after_restart"),
        "worker_failures": ("prepare_failed", "prepare_retry", "cleanup"),
        "repository_routing": ("same_numbers", "different_targets"),
    },
    "cli": {"intake": ("defaults", "overrides")},
}
TOKENS = {"<BRAIN>", "<HOME>", "<SOCKETS>", "<WORKSPACE>", "<SOURCE>", "<EXECUTABLE>"}
EVIDENCE = {"inputs", "calls", "posts", "events", "states", "relations", "assertions"}
CALL = {"name", "args", "dir", "env", "artifacts"}


class Invalid(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise Invalid(message)


def exact(value, keys, where):
    require(type(value) is dict and set(value) == set(keys), f"{where}: expected exactly {sorted(keys)}")


def strings(value, where, nonempty=False):
    require(type(value) is list and all(type(x) is str for x in value), f"{where}: expected string array")
    if nonempty:
        require(bool(value) and all(value), f"{where}: empty labels")


def string_map(value, where):
    require(type(value) is dict and all(type(k) is str and type(v) is str for k, v in value.items()),
            f"{where}: expected string map")


def no_review_request(call, where):
    # Reviewer assignment is different from authoring a review or reading one.
    args = call["args"]
    name = Path(call["name"]).name
    forbidden = False
    if name == "gh":
        create = any(args[i:i + 2] == ["pr", "create"] for i in range(len(args) - 1))
        edit = any(args[i:i + 2] == ["pr", "edit"] for i in range(len(args) - 1))
        if create:
            forbidden = any(x == "--reviewer" or x.startswith("--reviewer=") or x.startswith("-r") for x in args)
        if edit:
            forbidden = forbidden or any(x == "--add-reviewer" or x.startswith("--add-reviewer=") for x in args)
        if "api" in args:
            method = None
            for i, arg in enumerate(args):
                if arg in {"-X", "--method"} and i + 1 < len(args):
                    method = args[i + 1].upper()
                elif arg.startswith("--method="):
                    method = arg.split("=", 1)[1].upper()
                elif arg.startswith("-X") and len(arg) > 2:
                    method = arg[2:].lstrip("=").upper()
            if method is None:
                body = any(x in {"-f", "-F", "--field", "--raw-field", "--input"} or
                           x.startswith(("--field=", "--raw-field=", "--input=", "-f", "-F")) for x in args)
                method = "POST" if body else "GET"
            assignments = any(re.search(r"/pulls/[0-9]+/requested_reviewers(?:[?/#]|$)", x) for x in args)
            graphql = any(re.search(r"\brequestReviews\s*\(", x) for x in args)
            forbidden = forbidden or (assignments and method == "POST") or graphql
    require(not forbidden, f"{where}: personal-repository review request forbidden")
    require(not any("ask-reviews" in x or "slack-dm" in x for x in [call["name"], *args]),
            f"{where}: Slack/review request forbidden")


def validate_trace(trace, package):
    require(package in INVENTORY, "unknown package")
    exact(trace, {"schema_version", "package", "scenarios"}, "trace")
    require(type(trace["schema_version"]) is int and trace["schema_version"] == 1, "schema_version must be integer 1")
    require(trace["package"] == package, "wrong package")
    scenarios = trace["scenarios"]
    require(type(scenarios) is list, "scenarios must be array")
    names = []
    for scenario in scenarios:
        exact(scenario, {"name", "steps"}, "scenario")
        name = scenario["name"]
        require(type(name) is str and name in INVENTORY[package], "unknown scenario")
        names.append(name)
        require(type(scenario["steps"]) is list, f"{name}: steps must be array")
        step_names, operations, relationships = [], 0, 0
        for step in scenario["steps"]:
            exact(step, {"name", "evidence"}, name)
            require(type(step["name"]) is str, "step name must be string")
            step_names.append(step["name"])
            where = f"{package}/{name}/{step['name']}"
            e = step["evidence"]
            exact(e, EVIDENCE, where)
            strings(e["inputs"], where + "/inputs", nonempty=True)
            strings(e["posts"], where + "/posts")
            strings(e["assertions"], where + "/assertions", nonempty=True)
            require(type(e["calls"]) is list, where + ": calls must be array")
            for call in e["calls"]:
                exact(call, CALL, where + "/call")
                require(type(call["name"]) is str and bool(call["name"]), where + ": empty call name")
                require(type(call["dir"]) is str, where + ": call dir must be string")
                strings(call["args"], where + "/args")
                strings(call["env"], where + "/env")
                string_map(call["artifacts"], where + "/artifacts")
                no_review_request(call, where)
            require(type(e["events"]) is dict, where + ": events must be entity map")
            for entity, events in e["events"].items():
                require(type(entity) is str and bool(entity) and type(events) is list and
                        all(type(event) is dict and bool(event) for event in events), where + ": malformed event records")
            require(type(e["states"]) is dict and all(type(k) is str and bool(k) and type(v) is dict and bool(v)
                    for k, v in e["states"].items()), where + ": malformed state records")
            string_map(e["relations"], where + "/relations")
            count = len(e["calls"]) + len(e["posts"]) + sum(len(x) for x in e["events"].values())
            operations += count
            relationships += len(e["relations"])
            require(count or e["states"] or e["relations"], where + ": empty evidence")
        require(step_names == list(INVENTORY[package][name]), f"{name}: missing, duplicate or reordered steps")
        require(operations and relationships, f"{name}: no observed operations/relationships")
    require(names == list(INVENTORY[package]), "missing, duplicate or reordered scenarios")
    return trace


def parse_json(text, where):
    def pairs(items):
        result = {}
        for k, v in items:
            require(k not in result, f"{where}: duplicate JSON key {k}")
            result[k] = v
        return result
    def constant(value):
        raise Invalid(f"{where}: non-JSON numeric constant {value}")
    def number(value):
        parsed = float(value)
        require(math.isfinite(parsed), f"{where}: nonfinite number {value}")
        return parsed
    return json.loads(text, object_pairs_hook=pairs, parse_constant=constant, parse_float=number)


def load_json(path):
    return parse_json(Path(path).read_text(), str(path))


def normalize(value, roots):
    """Replace only declared root prefixes; keep suffixes, array order and all fields."""
    if type(value) is str:
        for root in sorted(roots, key=len, reverse=True):
            value = re.sub(r'(?<![A-Za-z0-9_./-])' + re.escape(root) + r'(?=$|[/\s"\'=:,;)}\]])',
                           lambda _: roots[root], value)
        return value
    if type(value) is list:
        return [normalize(x, roots) for x in value]
    if type(value) is dict:
        result = {}
        for key, child in value.items():
            new_key = normalize(key, roots)
            require(new_key not in result, "normalization would collapse distinct keys")
            result[new_key] = normalize(child, roots)
        return result
    return value


def within(path, parent):
    return path == parent or parent in path.parents



def event_time(value, where):
    # Compare fractional seconds exactly; datetime alone truncates Go nanoseconds.
    require(type(value) is str, where + ": at must be RFC3339 string")
    match = re.fullmatch(r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]+))?(Z|[+-][0-9]{2}:[0-9]{2})", value)
    require(match is not None, where + ": at must be RFC3339 string")
    seconds, fraction, zone = match.groups()
    if zone != "Z":
        require(int(zone[1:3]) <= 23 and int(zone[4:6]) <= 59, where + ": invalid RFC3339 offset")
    try:
        instant = datetime.datetime.fromisoformat(seconds + ("+00:00" if zone == "Z" else zone))
        instant = instant.astimezone(datetime.timezone.utc)
    except (ValueError, OverflowError) as error:
        raise Invalid(where + ": invalid RFC3339 timestamp") from error
    return instant, (fraction or "").rstrip("0")



# Each shape is backed by a specific run.append/once call without At. This is
# not a general allowance for factory-authored events or event timestamps.
PROGRAM_EVENTS = {
    "checked_plan": ("done", r"checked-plan:([1-9][0-9]*)"),
    "mirror": ("done", r"mirror:([1-9][0-9]*)"),
    "assignment": ("done", r"assign:([A-Za-z0-9_-]+)"),
    "handover": ("handover", r"handover:([1-9][0-9]*)"),
    "cancel_done": ("done", r"cancel:([1-9][0-9]*)"),
    "stop_propagation": ("request", r"stop:([A-Za-z0-9_-]+):([1-9][0-9]*)"),
    "cancel_cleanup": ("request", r"repo:cleanup:cancel:([1-9][0-9]*)"),
    "cancel_unassign": ("done", r"cancel:([1-9][0-9]*):unassign"),
    "stop_refused": ("refused", r"refused:([1-9][0-9]*)"),
    "cancel_message": ("message", r"cancel:([1-9][0-9]*):tell"),
}


def fixture_event(evidence, entity, event):
    seq = event.get("seq")
    if f"fixture_event:{entity}#{seq}" in evidence["relations"]:
        return True
    prefix = f"fixture-event:{entity}:"
    for label in evidence["inputs"]:
        if label.startswith(prefix):
            record = parse_json(label[len(prefix):], "fixture event input")
            if type(record) is dict and type(record.get("seq")) is int and record["seq"] == seq:
                return True
    return False


def program_family(event):
    if event.get("sender") != "factory" or type(event.get("key")) is not str:
        return None
    for family, (kind, pattern) in PROGRAM_EVENTS.items():
        if event.get("kind") == kind and re.fullmatch(pattern, event["key"]):
            return family
    return None


def evidence_event(evidence, entity, seq):
    require(type(seq) is int and seq > 0, "event reference must be positive integer")
    events = evidence["events"].get(entity, [])
    require(all(type(e.get("seq")) is int and e["seq"] == i + 1 for i, e in enumerate(events)),
            "program event log must have contiguous unique integer sequences")
    require(seq <= len(events), "program event target/reference absent")
    return events[seq - 1]


def entity_record(evidence, entity):
    value = evidence["relations"].get("index:" + entity, "").split(":", 2)
    require(len(value) == 3 and value[0] in {"epic", "set", "work", "session"} and Path(value[2]).is_absolute(),
            "missing/malformed entity index evidence")
    return value


def fixture_inputs(evidence, entity):
    # Read only the simple scalar/list fields emitted by the frozen Go structs.
    # This is deliberately not a general YAML decoder; unsupported forms fail.
    body = evidence["relations"].get("inputs:" + entity)
    require(type(body) is str, "missing actual entity inputs")
    fields, list_key = {}, None
    selected = {"id", "epic", "set", "repo", "issue", "pr", "task", "component", "items"}
    for line in body.splitlines():
        match = re.fullmatch(r"([a-z_]+):(?:[ \t]*(.*))?", line)
        if match:
            key, value = match[1], (match[2] or "").strip()
            list_key = None
            if key not in selected:
                continue
            require(key not in fields, "duplicate entity input field")
            if key == "items":
                require(value == "" or (value.startswith("[") and value.endswith("]")), "unsupported set items")
                fields[key] = [] if value == "" else [x.strip() for x in value[1:-1].split(",") if x.strip()]
                list_key = key if value == "" else None
            else:
                require(re.fullmatch(r"[A-Za-z0-9_./#-]+", value) is not None, "unsupported entity input scalar")
                fields[key] = value
        elif list_key and line.strip().startswith("- "):
            item = line.strip()[2:].strip()
            require(re.fullmatch(r"[A-Za-z0-9_-]+", item) is not None, "invalid set item ID")
            fields[list_key].append(item)
    if "id" in fields:
        require(fields["id"] == entity, "entity inputs belong to another ID")
    return fields


def plan_link_count(body):
    # The agreed plans use flat flow-map items/links. Full bytes remain hashed
    # and compared; do not silently accept a new YAML shape by guessing at it.
    section, items, links = None, {}, []
    for line in body.splitlines():
        header = re.fullmatch(r"([a-z_]+):[ \t]*(.*)", line)
        if header:
            section = header[1]
            if section in {"items", "links"}:
                require(header[2].strip() in {"", "[]"}, "unsupported plan collection format")
            continue
        if section not in {"items", "links"} or not line.strip():
            continue
        match = re.fullmatch(r"[ \t]+-[ \t]+\{([^{}\[\]\"']*)\}[ \t]*", line)
        require(match is not None, "unsupported plan item/link format")
        fields = {}
        for pair in match[1].split(","):
            key, separator, value = pair.partition(":")
            key, value = key.strip(), value.strip()
            require(separator and key not in fields and value, "malformed/duplicate plan flow field")
            fields[key] = value
        if section == "items":
            require({"id", "repo", "issue"} <= fields.keys() and fields["id"] not in items,
                    "missing/duplicate plan item")
            items[fields["id"]] = fields["issue"]
        else:
            require({"from", "to"} <= fields.keys(), "missing plan link endpoints")
            links.append((fields["from"], fields["to"]))
    require(bool(items), "plan has no supported items")
    require(all(a in items and b in items for a, b in links), "plan link references unknown item")
    return len({(items[b], items[a]) for a, b in links if items[a] != items[b]})


def program_tick(evidence, entity, seq, calls):
    value = evidence["relations"].get(f"event_tick:{entity}#{seq}", "")
    require(re.fullmatch(r"[1-9][0-9]*", value) is not None, "missing program tick provenance")
    tick = int(value)
    span = parse_json(evidence["relations"].get(f"tick_calls:{tick}", "null"), "tick calls")
    exact(span, {"start", "end"}, "tick calls")
    start, end = span["start"], span["end"]
    require(type(start) is int and type(end) is int and 0 <= start <= end <= len(calls), "invalid tick call range")
    return tick, start, end


def cancelled_cause(evidence, entity, ref):
    transition = evidence_event(evidence, entity, ref)
    require(transition.get("kind") == "transition" and transition.get("to") == "cancelled" and
            transition.get("trigger") == "user", "missing cancelled transition")
    source = transition.get("trigger_ref")
    require(type(source) is str and re.fullmatch(r"[1-9][0-9]*", source) is not None,
            "cancel transition has invalid request reference")
    request = evidence_event(evidence, entity, int(source))
    require(int(source) < ref and request.get("kind") == "request" and request.get("request") == "stop" and
            request.get("to") == "cancelled" and request.get("trigger") == "user" and
            type(request.get("text", "")) is str, "cancel transition has no earlier stop request")
    return transition, request


def captured_read(evidence, calls, index, tick, kind, repo="", number=""):
    value = parse_json(evidence["relations"].get(f"read_call:{index}", "null"), "captured read")
    exact(value, {"call_index", "tick", "kind", "repo", "pr", "sha", "stdout", "error"}, "captured read")
    require(type(value["call_index"]) is int and type(value["tick"]) is int and
            value["call_index"] == index and value["tick"] == tick, "read call/tick provenance mismatch")
    require(all(type(value[k]) is str for k in ("kind", "repo", "pr", "sha", "stdout", "error")), "read field type mismatch")
    require((value["kind"], value["repo"], value["pr"], value["sha"], value["error"]) ==
            (kind, repo, number, "", ""), "read target/kind/error mismatch")
    args = ["api", "user"] if kind == "me" else ["api", f"repos/{repo}/issues/{number}"]
    require(calls[index]["name"] == "gh" and calls[index]["args"] == args, "read is not the recorded actual command")
    result = parse_json(value["stdout"], "captured stdout")
    require(type(result) is dict, "read response must be object")
    return result


def verify_program_event(evidence, calls, entity, event, family):
    seq, at = event["seq"], event_time(event.get("at"), "program event")
    require(not fixture_event(evidence, entity, event), "fixture event is not eligible")
    require(type(event.get("text", "")) is str, "program event text must be string")
    require(program_family(event) == family, "unsupported program family or wrong sender/kind/key")
    tick, start, end = program_tick(evidence, entity, seq, calls)
    kind, epic, directory = entity_record(evidence, entity)
    match = re.fullmatch(PROGRAM_EVENTS[family][1], event["key"])
    ref = int(match[1]) if family not in {"assignment", "stop_propagation"} else None
    earlier = evidence["events"][entity][:seq - 1]

    def prior(number, owner=entity):
        result = evidence_event(evidence, owner, number)
        require((owner != entity or number < seq) and at >= event_time(result.get("at"), "program reference"),
                "missing/out-of-order earlier program reference")
        return result

    if family in {"checked_plan", "mirror"}:
        require(kind == "epic", "plan event must belong to epic")
        source = prior(ref)
        filename = Path(source.get("file", ""))
        require(source.get("kind") == "end" and re.fullmatch(r"epic\.v[1-9][0-9]*\.yaml", filename.name) is not None and
                filename.parent == Path(directory), "plan key does not name this epic's ended file")
        body = evidence["relations"].get(f"plan_content:{entity}#{ref}")
        require(type(body) is str and digest(body.encode()) == source.get("sha256"), "missing/mismatched actual ended plan content")
        count = plan_link_count(body)
        if family == "checked_plan":
            require(event.get("text") == "checked " + filename.name, "checked plan text mismatch")
        else:
            require(any(e.get("kind") == "transition" and e.get("to") == "planned" and
                        e.get("trigger_ref") == str(ref) and at >= event_time(e.get("at"), "applied plan") for e in earlier),
                    "mirror lacks applied-plan transition")
            require(event.get("text") == f"mirror {count} blocked-by links of {entity} to GitHub", "mirror plan/count mismatch")
        return
    if family == "assignment":
        require(kind == "work" and match[1] == entity and event.get("text") in {"assigned", "already assigned"},
                "invalid assignment entity/text")
        starts = [e for e in earlier if e.get("kind") == "transition" and e.get("to") == "starting"]
        require(starts and at >= event_time(starts[0].get("at"), "work start"), "assignment precedes starting")
        inputs = fixture_inputs(evidence, entity)
        repo, number = inputs.get("repo"), inputs.get("issue", "")
        require(type(repo) is str and re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo) is not None and
                re.fullmatch(r"[1-9][0-9]*", number) is not None, "invalid assignment repository/issue")
        for i in range(start, end):
            if calls[i]["name"] != "gh" or calls[i]["args"] != ["api", f"repos/{repo}/issues/{number}"]:
                continue
            issue = captured_read(evidence, calls, i, tick, "issue", repo, number)
            require(type(issue.get("number")) is int and issue["number"] == int(number) and
                    issue.get("repository_url") == "https://api.github.com/repos/" + repo, "issue response target mismatch")
            users = issue.get("assignees")
            if users is None:
                users = []
            require(type(users) is list and all(type(u) is dict and type(u.get("login")) is str and u["login"] for u in users),
                    "invalid issue assignees")
            for j in range(start, i):
                if calls[j]["name"] != "gh" or calls[j]["args"] != ["api", "user"]:
                    continue
                me = captured_read(evidence, calls, j, tick, "me").get("login")
                require(type(me) is str and me, "missing actual current-user response")
                assigned = any(u["login"] == me for u in users)
                if event["text"] == "already assigned" and assigned:
                    return
                if event["text"] == "assigned" and not assigned and any(c["name"] == "gh" and c["args"] ==
                        ["issue", "edit", number, "-R", repo, "--add-assignee", "@me"] for c in calls[i + 1:end]):
                    return
        raise Invalid("assignment lacks matching issue/me response and action in its tick")
    if family == "handover":
        require(kind == "work" and event.get("text") == "the pr-opener ended", "unsupported handover entity/reason")
        transition = prior(ref)
        require(transition.get("kind") == "transition" and transition.get("to") == "pr_open", "handover lacks pr_open transition")
        require(any(e.get("kind") == "end" and Path(e.get("file", "")) == Path(directory) / "pr.md" and
                    e["seq"] > ref and at >= event_time(e.get("at"), "pr opener end") for e in earlier),
                "handover lacks earlier pr-opener end")
        return
    if family in {"cancel_done", "cancel_cleanup", "cancel_unassign"}:
        require(kind == "work" or (family == "cancel_done" and kind in {"epic", "set"}), "invalid cancellation entity type")
        transition, request = cancelled_cause(evidence, entity, ref)
        prior(ref)
        require(at >= event_time(request.get("at"), "stop request"), "cancellation precedes stop request")
        if family == "cancel_done":
            require(event.get("text") == "cancelled: factory stop: " + request.get("text", ""), "cancel reason mismatch")
            return
        inputs = fixture_inputs(evidence, entity)
        if family == "cancel_cleanup":
            require(event.get("recipient") == "repo-worker" and event.get("request") == "cleanup" and
                    event.get("text") == "cancelled" and inputs.get("pr", "0") == "0", "invalid cancelled cleanup request")
            require(any(e.get("kind") == "request" and e.get("recipient") == "repo-worker" and
                        e.get("request") == "worktree" for e in earlier), "cancel cleanup lacks earlier worktree request")
            return
        repo, number = inputs.get("repo"), inputs.get("issue")
        require(repo and number and event.get("text") == f"unassign {repo}#{number}", "unassignment target mismatch")
        require(any(e.get("kind") == "done" and e.get("key") == "assign:" + entity and e.get("text") == "assigned" and
                    e["seq"] < ref and not fixture_event(evidence, entity, e) for e in earlier), "unassignment lacks factory assignment")
        require(any(c["name"] == "gh" and c["args"] == ["issue", "edit", number, "-R", repo, "--remove-assignee", "@me"]
                    for c in calls[start:end]), "unassignment lacks matching removal call")
        return
    if family == "stop_propagation":
        parent, number = match[1], int(match[2])
        require(kind in {"work", "set"}, "stop propagation target must be work or set")
        transition, request = cancelled_cause(evidence, parent, number)
        prior(number, parent)
        require(at >= event_time(request.get("at"), "parent stop request"), "propagation precedes stop request")
        require(event.get("request") == "stop" and event.get("to") == "cancelled" and event.get("trigger") == "user" and
                event.get("recipient", "") == "" and event.get("text", "") == request.get("text", ""), "propagated stop payload mismatch")
        parent_kind = entity_record(evidence, parent)[0]
        if parent_kind == "epic" and epic == parent:
            return
        if parent_kind == "set" and kind == "work" and entity in fixture_inputs(evidence, parent).get("items", []):
            return
        raise Invalid("propagated stop parent does not own child")
    if family == "stop_refused":
        require(kind == "work" and all(event.get(k) == v for k, v in {"from": "cancelled", "to": "cancelled",
                "trigger": "user", "trigger_ref": str(ref), "text": "cancelled → cancelled on user: not an allowed move"}.items()),
                "unsupported refused stop shape")
        request = prior(ref)
        require(request.get("kind") == "request" and request.get("request") == "stop" and request.get("to") == "cancelled" and
                request.get("trigger") == "user", "refusal lacks earlier stop request")
        require(any(e.get("kind") == "transition" and e.get("to") == "cancelled" and at >= event_time(e.get("at"), "cancelled state")
                    for e in earlier), "refusal precedes cancelled state")
        return
    require(family == "cancel_message" and kind == "set", "cancel message must belong to set")
    session = False
    for sid in evidence["events"]:
        if evidence["relations"].get("index:" + sid, "").startswith("session:"):
            inputs = fixture_inputs(evidence, sid)
            session = session or (inputs.get("task") == entity and inputs.get("component") == "orchestrator")
    require(session, "cancel message set lacks orchestrator")
    for wid in evidence["events"]:
        if not evidence["relations"].get("index:" + wid, "").startswith("work:"):
            continue
        if fixture_inputs(evidence, wid).get("set") != entity:
            continue
        # A set can contain several items; only the text-named item is the cause.
        if not event.get("text", "").startswith(wid + " is cancelled ("):
            continue
        transition, request = cancelled_cause(evidence, wid, ref)
        prior(ref, wid)
        require(at >= event_time(request.get("at"), "work stop request"), "message precedes stop request")
        require(event.get("text") == f"{wid} is cancelled (factory stop: {request.get('text', '')}): stop work on it; the program removes its worktree.",
                "cancel message reason mismatch")
        return
    raise Invalid("cancel message lacks cancelled work/ownership/reason link")


def normalize_event_times(trace, identities):
    require(type(identities) is dict and set(identities) <= {"worker_event_times", "program_event_times"},
            "unsupported timestamp identity envelope")
    workers = identities.get("worker_event_times", [])
    programs = identities.get("program_event_times", [])
    require(type(workers) is list and type(programs) is list, "timestamp identity lists must be arrays")
    result = normalize_worker_times(trace, {"worker_event_times": workers} if workers else {})
    worker_coordinates = {(e["scenario"], e["step"], e["entity"], e["seq"]) for e in workers}
    indexed, expected = {}, {}
    for scenario in trace["scenarios"]:
        calls = []
        for step in scenario["steps"]:
            evidence = step["evidence"]
            calls.extend(evidence["calls"])
            for entity, events in evidence["events"].items():
                for event in events:
                    family = program_family(event)
                    if not family or fixture_event(evidence, entity, event):
                        continue
                    coordinate = (scenario["name"], step["name"], entity, event.get("seq"))
                    evidence_event(evidence, entity, event.get("seq"))
                    require(coordinate not in expected, "duplicate program event target")
                    expected[coordinate] = family
            indexed[scenario["name"], step["name"]] = (evidence, list(calls))
    seen = set()
    for entry in programs:
        exact(entry, {"scenario", "step", "entity", "seq", "at", "family"}, "program timestamp identity")
        require(all(type(entry[k]) is str and bool(entry[k]) for k in ("scenario", "step", "entity", "at", "family")) and
                type(entry["seq"]) is int and entry["seq"] > 0, "program timestamp identity has wrong types")
        require(trace["package"] == "reconcile" and entry["family"] in PROGRAM_EVENTS, "unsupported program timestamp family/package")
        coordinate = (entry["scenario"], entry["step"], entry["entity"], entry["seq"])
        require(coordinate not in worker_coordinates, "overlapping worker/program timestamp identities")
        require(coordinate not in seen, "duplicate program timestamp identity")
        require((entry["scenario"], entry["step"]) in indexed, "program timestamp scenario/step absent")
        evidence, calls = indexed[entry["scenario"], entry["step"]]
        event = evidence_event(evidence, entry["entity"], entry["seq"])
        require(not fixture_event(evidence, entry["entity"], event), "fixture event is not eligible")
        require(expected.get(coordinate) == entry["family"] and event.get("at") == entry["at"], "program family/target/timestamp mismatch")
        verify_program_event(evidence, calls, entry["entity"], event, entry["family"])
        seen.add(coordinate)
    require(seen == set(expected), "missing program timestamp identities")
    for scenario in result["scenarios"]:
        for step in scenario["steps"]:
            for entity, events in step["evidence"]["events"].items():
                for event in events:
                    if type(event.get("seq")) is int and (scenario["name"], step["name"], entity, event["seq"]) in seen:
                        event["at"] = f"<PROGRAM_AT:{entity}:{event['seq']}>"
    return result


def normalize_worker_times(trace, identities):
    """Validate request/result provenance, then change only each mapped result.at."""
    require(type(identities) is dict, "identities must be object")
    if identities:
        exact(identities, {"worker_event_times"}, "identities")
        entries = identities["worker_event_times"]
        require(type(entries) is list, "worker_event_times must be array")
    else:
        entries = []
    expected = {}
    for scenario in trace["scenarios"]:
        for step in scenario["steps"]:
            for entity, events in step["evidence"]["events"].items():
                for position, event in enumerate(events):
                    kind, key = event.get("kind"), event.get("key")
                    # Requests also have repo:worktree/cleanup:<seq> keys, but
                    # only result timestamps are eligible for this exception.
                    # A malformed result must not evade provenance validation by
                    # dropping its identity entry.
                    if kind != "worktree_ready" and not (kind != "request" and type(key) is str and key.startswith("repo:")):
                        continue
                    where = f"{scenario['name']}/{step['name']}/{entity}"
                    require(not fixture_event(step["evidence"], entity, event), "fixture worker event is not eligible")
                    seq, trigger = event.get("seq"), event.get("trigger_ref")
                    require(type(seq) is int and seq > 0, where + ": worker result seq must be positive integer")
                    require(event.get("sender") == "factory" and type(kind) is str and kind in {"worktree_ready", "done", "error"},
                            where + ": wrong worker result sender/kind")
                    require(type(trigger) is str and re.fullmatch(r"[1-9][0-9]*", trigger) is not None and
                            key == "repo:" + trigger, where + ": wrong worker result key/trigger_ref")
                    coordinate = (scenario["name"], step["name"], entity, seq)
                    require(coordinate not in expected and
                            sum(type(e.get("seq")) is int and e["seq"] == seq for e in events) == 1,
                            where + ": duplicate worker result seq")
                    requests = [(i, e) for i, e in enumerate(events)
                                if type(e.get("seq")) is int and str(e["seq"]) == trigger]
                    require(len(requests) == 1, where + ": missing/duplicate worker request")
                    request_position, request = requests[0]
                    require(request_position < position and request["seq"] < seq and
                            request.get("kind") == "request" and request.get("recipient") == "repo-worker" and
                            type(request.get("request")) is str and request["request"] in {"worktree", "cleanup"},
                            where + ": worker request provenance/order mismatch")
                    require(kind == "error" or (kind == "worktree_ready" and request["request"] == "worktree") or
                            (kind == "done" and request["request"] == "cleanup"),
                            where + ": worker result does not match requested operation")
                    require(event_time(event.get("at"), where + "/result") >=
                            event_time(request.get("at"), where + "/request"),
                            where + ": worker result precedes request timestamp")
                    expected[coordinate] = event
    seen = set()
    for entry in entries:
        exact(entry, {"scenario", "step", "entity", "seq", "at"}, "worker timestamp identity")
        require(all(type(entry[k]) is str and bool(entry[k]) for k in ("scenario", "step", "entity", "at")) and
                type(entry["seq"]) is int and entry["seq"] > 0, "worker timestamp identity has wrong types")
        coordinate = (entry["scenario"], entry["step"], entry["entity"], entry["seq"])
        require(coordinate not in seen, "duplicate worker timestamp identity")
        require(coordinate in expected, "worker timestamp identity has absent/wrong-provenance target")
        require(entry["at"] == expected[coordinate]["at"], "worker timestamp identity at differs from raw event")
        seen.add(coordinate)
    require(seen == set(expected), "missing worker timestamp identities")
    result = copy.deepcopy(trace)
    for scenario in result["scenarios"]:
        for step in scenario["steps"]:
            for entity, events in step["evidence"]["events"].items():
                for event in events:
                    coordinate = (scenario["name"], step["name"], entity, event.get("seq"))
                    # Other official event fields may have unrelated shapes. Only
                    # worker targets validated above are eligible for replacement.
                    if type(event.get("seq")) is int and coordinate in seen:
                        event["at"] = f"<WORKER_AT:{entity}:{event['seq']}>"
    return result



def validate_roots(raw, package, source, private):
    """Pin the map to newWorld/cliCompatNew and Go testing.TempDir's maker layout."""
    roots = raw["roots"]
    string_map(roots, "raw roots")
    worlds = len(INVENTORY["reconcile"]) if package == "reconcile" else 1
    expected_tokens = {"<SOURCE>": 1, "<BRAIN>": worlds, "<HOME>": worlds}
    if package == "reconcile":
        expected_tokens["<SOCKETS>"] = worlds
    counts, test_roots, sockets, parents = {}, {}, [], set()
    for root, token in roots.items():
        require(Path(root).is_absolute() and os.path.normpath(root) == root,
                "noncanonical normalization root")
        require(token in expected_tokens, "token absent from actual fixture root inventory")
        counts[token] = counts.get(token, 0) + 1
        path = Path(root)
        if token == "<SOURCE>":
            require(path == source, "SOURCE root must be pinned source tree")
            continue
        relative = path.relative_to(private / "tmp") if within(path, private / "tmp") else None
        require(relative is not None, "fixture root outside owned TMPDIR")
        if token == "<SOCKETS>":
            require(len(relative.parts) == 1 and re.fullmatch(r"socks[0-9]{1,10}", relative.name) is not None,
                    "SOCKETS root must be actual direct MkdirTemp child")
            sockets.append(path)
            continue
        require(len(relative.parts) == 2 and
                re.fullmatch(r"TestClaudeCompatCapture[0-9]{1,10}", relative.parts[0]) is not None and
                re.fullmatch(r"[0-9]{3}", relative.parts[1]) is not None,
                "brain/home root must be exact numbered testing.TempDir child; nested shadow forbidden")
        index = int(relative.parts[1])
        require(1 <= index <= 2 * worlds and token == ("<BRAIN>" if index % 2 else "<HOME>"),
                "brain/home root does not match fixture maker order")
        require(index not in test_roots, "duplicate fixture root index")
        test_roots[index] = path
        parents.add(path.parent)
    require(counts == expected_tokens and len(parents) == 1 and len(test_roots) == 2 * worlds,
            "normalization roots differ from exact fixture inventory/counts")
    paths = [Path(p) for p in roots]
    require(not any(a != b and within(a, b) for a in paths for b in paths), "nested normalization shadow root")
    # In reconcile each scenario creates its own world, in frozen scenario order.
    # CLI uses one world for both intake steps. Require the actual roots in that
    # scenario's evidence, rather than allowing unused invented maker directories.
    for index, scenario in enumerate(raw["trace"]["scenarios"]):
        world = index if package == "reconcile" else 0
        text = json.dumps(scenario, ensure_ascii=False)
        require(all(str(test_roots[n]) in text for n in (2 * world + 1, 2 * world + 2)),
                "scenario evidence does not use its declared maker roots")
        for n, path in test_roots.items():
            if n not in (2 * world + 1, 2 * world + 2):
                require(str(path) not in text, "scenario references another fixture world's root")
    return {"tokens": counts, "test_parent": str(next(iter(parents))),
            "numbered": {str(k): str(v) for k, v in test_roots.items()},
            "sockets": [str(p) for p in sockets]}


def validate_raw(raw, package, source, private, binary):
    exact(raw, {"trace", "roots", "identities", "cli_inputs"}, "raw artifact")
    validate_trace(raw["trace"], package)
    validate_roots(raw, package, source, private)
    raw_text = json.dumps(raw["trace"], ensure_ascii=False)
    require(not any(token in raw_text for token in TOKENS) and "<WORKER_AT:" not in raw_text and "<PROGRAM_AT:" not in raw_text,
            "raw trace already contains normalization tokens")
    require(type(raw["cli_inputs"]) is dict, "raw cli_inputs must be map")
    expected = {"intake/defaults", "intake/overrides"} if package == "cli" else set()
    require(set(raw["cli_inputs"]) == expected, "raw CLI input inventory mismatch")
    for key, args in raw["cli_inputs"].items():
        strings(args, f"raw CLI inputs {key}", nonempty=True)
    return normalize(normalize_event_times(raw["trace"], raw["identities"]), raw["roots"])


def first_difference(a, b, where="$"):
    if type(a) is not type(b):
        return f"{where}: type {type(a).__name__} != {type(b).__name__}"
    if type(a) is dict:
        if a.keys() != b.keys():
            return f"{where}: keys {sorted(a)} != {sorted(b)}"
        for key in sorted(a):
            diff = first_difference(a[key], b[key], where + "/" + key)
            if diff:
                return diff
    elif type(a) is list:
        if len(a) != len(b):
            return f"{where}: length {len(a)} != {len(b)}"
        for i, (left, right) in enumerate(zip(a, b)):
            diff = first_difference(left, right, f"{where}[{i}]")
            if diff:
                return diff
    elif a != b:
        return f"{where}: {a!r} != {b!r}"
    return None


def compare_cli_inputs(main, candidate, explicit, main_roots, candidate_roots):
    require(set(main) == set(candidate) == {"intake/defaults", "intake/overrides"}, "CLI input keys differ")
    for key in main:
        left, right = main[key], candidate[key]
        for args in (left, right):
            strings(args, key, nonempty=True)
        require(not any(x == "--executor" or x.startswith("--executor=") for x in left),
                key + ": baseline must be flagless")
        if explicit:
            require(right.count("--executor") == 1 and not any(x.startswith("--executor=") for x in right),
                    key + ": explicit executor flag required exactly once")
            pos = right.index("--executor")
            require(right[pos:pos + 2] == ["--executor", "claude"], key + ": expected --executor claude")
            # This removal applies ONLY to fixture inputs, never actual proc.Cmd args.
            right = right[:pos] + right[pos + 2:]
        else:
            require(not any(x == "--executor" or x.startswith("--executor=") for x in right),
                    key + ": default lane unexpectedly selects executor")
        diff = first_difference(normalize(left, main_roots), normalize(right, candidate_roots))
        require(not diff, f"{key}: unexpected raw CLI input difference: {diff}")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_hash(path):
    return digest(Path(path).read_bytes())


def private_env(private, go=None, cache=None, modules=None):
    env = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C", "TZ": "UTC",
           "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
           "GIT_TERMINAL_PROMPT": "0", "GIT_NO_REPLACE_OBJECTS": "1"}
    for name in ("home", "tmp", "brain", "sockets", "config", "cache"):
        (private / name).mkdir(parents=True, mode=0o700, exist_ok=True)
    env.update(HOME=str(private / "home"), TMPDIR=str(private / "tmp"),
               TMP=str(private / "tmp"), TEMP=str(private / "tmp"),
               FACTORY_BRAIN=str(private / "brain"),
               FACTORY_SOCKET_DIR=str(private / "sockets"),
               XDG_CONFIG_HOME=str(private / "config"), XDG_CACHE_HOME=str(private / "cache"))
    if go:
        env["PATH"] = str(Path(go).parent) + ":/usr/bin:/bin"
        env.update(GOENV="off", GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off",
                   GOWORK="off", GOFLAGS="-mod=readonly", GOCACHE=str(cache),
                   GOMODCACHE=str(modules), GOPATH=str(private / "gopath"),
                   CGO_ENABLED="0")
    return env


def bounded(args, cwd, env, timeout, log=None):
    """A timeout kills the entire process group, including compiler descendants."""
    with subprocess.Popen(args, cwd=cwd, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, start_new_session=True) as child:
        try:
            out, err = child.communicate(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            with contextlib.suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)
            out, err = child.communicate()
            if log:
                Path(log).write_bytes(out + b"\nSTDERR\n" + err)
            raise Invalid(f"command timed out/interrupted: {args!r}")
    if log:
        Path(log).write_bytes(out + b"\nSTDERR\n" + err)
    require(child.returncode == 0, f"command failed ({child.returncode}): {args!r}: {err.decode(errors='replace')}")
    return out


def git(repo, *args):
    env = {"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "GIT_CONFIG_NOSYSTEM": "1",
           "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0", "LC_ALL": "C",
           "GIT_NO_REPLACE_OBJECTS": "1"}
    return bounded(["/usr/bin/git", "--no-pager", "--no-optional-locks", "--no-replace-objects", *args], repo, env, 60)


def resolve(repo, revision):
    require(not revision.startswith("-"), "revision cannot be an option")
    sha = git(repo, "rev-parse", "--verify", revision + "^{commit}").decode().strip()
    require(bool(re.fullmatch(r"[0-9a-f]{40}", sha)), "expected full SHA-1 commit")
    return sha


def safe_path(name):
    p = PurePosixPath(name)
    require(not p.is_absolute() and bool(p.parts) and ".." not in p.parts and
            str(p) == name and ".git" not in p.parts, f"unsafe archive/overlay path: {name}")
    return p


def committed_tree(repo, sha):
    # Enumerate names first so forbidden credential/runtime paths are rejected
    # before requesting any file contents, including the archive itself.
    entries = git(repo, "ls-tree", "-r", "-z", sha).split(b"\0")
    inventory = {}
    forbidden = {".ssh", ".gnupg", ".aws", ".netrc", ".env.production"}
    for entry in entries:
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        name = name.decode()
        parts = safe_path(name).parts
        require(not forbidden.intersection(parts), f"forbidden committed credential path: {name}")
        require(kind == "blob" and mode in {"100644", "100755"}, f"unsupported Git entry: {name}")
        require(name not in inventory, f"duplicate Git entry: {name}")
        inventory[name] = (oid, int(mode[-3:], 8))
    archive = git(repo, "-c", "tar.umask=0022", "archive", "--format=tar", sha)
    files = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
        for member in tar:
            name = member.name.rstrip("/")
            safe_path(name)
            require(not member.issym() and not member.islnk(), f"archive links forbidden: {name}")
            if member.isdir():
                continue
            require(member.isfile() and name in inventory, f"unexpected archive member: {name}")
            require(name not in files, f"duplicate archive entry: {name}")
            files[name] = (tar.extractfile(member).read(), member.mode & 0o777)
    # The Git-object hashes are independent of the archive/staged-tree hashes.
    objects = {}
    require(set(inventory) == set(files), "archive differs from committed file inventory")
    for name, (oid, mode) in inventory.items():
        data = git(repo, "cat-file", "blob", oid)
        objects[name] = {"sha256": digest(data), "mode": mode, "git_blob": oid}
        if name in REPLACEABLE_HARNESS:
            objects[name]["content_base64"] = base64.b64encode(data).decode("ascii")
        archived, archive_mode = files[name]
        require(digest(archived) == objects[name]["sha256"] and archive_mode == mode,
                f"archive changed committed file: {name}")
    return files, objects


def agreed_overlay_paths():
    paths = SHARED + FIXTURES + CANDIDATE
    require(len(paths) == len(set(paths)), "duplicate overlay path")
    for name in paths:
        safe_path(name)
    require(all(name.startswith("internal/reconcile/testdata/claude_compat/") for name in FIXTURES),
            "fixture overlay must stay in the exact agreed claude_compat subtree")
    return paths


def overlay_collision_message(name):
    return (f"tracked fixture differs from agreed bytes/mode: {name}. "
            "Existing fixtures cannot be overwritten; select matching fixture bytes or a pre-fixture revision.")


def read_overlay(repo, trees):
    overlay = {}
    for name in agreed_overlay_paths():
        path = repo / name
        require(path.is_file() and not path.is_symlink() and path.resolve().is_relative_to(repo.resolve()),
                f"overlay missing/not regular/escapes checkout: {name}")
        require(stat.S_IMODE(path.stat().st_mode) == 0o644, f"overlay must be regular mode 0644: {name}")
        data = path.read_bytes()
        for tree in trees:
            if name not in tree:
                continue
            record = tree[name]
            message = f"committed overlay source provenance mismatch: {name}"
            require(type(record.get("mode")) is int and record["mode"] in {0o644, 0o755} and type(record.get("sha256")) is str and
                    type(record.get("git_blob")) is str and re.fullmatch(r"[0-9a-f]{40}", record["git_blob"]) is not None,
                    message)
            original = git(repo, "cat-file", "blob", record["git_blob"])
            require(digest(original) == record["sha256"], message)
            if name in REPLACEABLE_HARNESS:
                require(record.get("content_base64") == base64.b64encode(original).decode("ascii"), message)
            else:
                require(record["mode"] == 0o644 and original == data, overlay_collision_message(name))
        overlay[name] = data
    return overlay


def check_dirty(repo):
    for entry in git(repo, "ls-files", "-v", "-z").split(b"\0"):
        require(not entry or entry[:1] == b"H", "hidden/skip-worktree/assume-unchanged index state forbidden")
    allowed = set(agreed_overlay_paths()) | OWN_FILES
    # -z reports rename source/destination separately. Renames/copies are never overlays.
    records = git(repo, "status", "--porcelain=v1", "-z", "--untracked-files=all").split(b"\0")
    for record in records:
        if not record:
            continue
        text = record.decode()
        require("R" not in text[:2] and "C" not in text[:2], "dirty rename/copy forbidden")
        name = text[3:]
        require(name in allowed, f"dirty production/unagreed file: {name}")
        require(name in OWN_FILES or text[:2] in {"??", "A ", " A", "AM", " M", "M ", "MM"},
                f"unsupported overlay status (deletion/conflict forbidden): {name}")
    # Also discover ignored files inside the harness subtree rather than silently skipping them.
    root = repo / "internal/reconcile/testdata/claude_compat"
    if root.exists():
        actual = {str(x.relative_to(repo)) for x in root.rglob("*") if x.is_file() or x.is_symlink()}
        require(actual == set(FIXTURES), "fixture inventory not explicitly agreed")


def tree_manifest(root):
    result = {}
    for path in root.rglob("*"):
        require(not path.is_symlink(), f"staged symlink: {path}")
        if path.is_file():
            result[str(path.relative_to(root))] = {"sha256": file_hash(path), "mode": path.stat().st_mode & 0o777}
    return result


def stage(root, files, source_manifest, overlay):
    require(set(overlay) <= set(agreed_overlay_paths()), "unagreed overlay path; production fixtures cannot be replaced")
    for name, data in overlay.items():
        if name in files and name not in REPLACEABLE_HARNESS:
            require(files[name] == (data, 0o644), overlay_collision_message(name))
        if name in files and name in REPLACEABLE_HARNESS:
            require(source_manifest.get(name, {}).get("content_base64") == base64.b64encode(files[name][0]).decode("ascii"),
                    f"original test harness bytes missing/mismatched in source manifest: {name}")
    root.mkdir(mode=0o700)
    for name, (data, mode) in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        target.chmod(mode)
    untouched = tree_manifest(root)
    expected = {k: {"sha256": v["sha256"], "mode": v["mode"]} for k, v in source_manifest.items()}
    require(untouched == expected, "untouched-source provenance mismatch")
    for name, data in overlay.items():
        target = root / name
        if name in files:
            require(target.is_file() and not target.is_symlink() and target.read_bytes() == files[name][0] and
                    stat.S_IMODE(target.stat().st_mode) == files[name][1], f"staged source changed before overlay: {name}")
            if files[name] == (data, 0o644):
                continue
            require(name in REPLACEABLE_HARNESS, overlay_collision_message(name))
            # Replace only an owned staged copy, never the checkout or Git blob.
            # The exact old bytes/mode remain in the original source manifest.
            with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".compat-overlay-", delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(data)
            temporary.chmod(0o644)
            os.replace(temporary, target)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(data)
        target.chmod(0o644)
    expected.update({k: {"sha256": digest(v), "mode": 0o644} for k, v in overlay.items()})
    require(tree_manifest(root) == expected, "staged source/overlay provenance mismatch")
    return expected


def overlay_for_lane(lane, source_manifest, overlay):
    require(lane in LANES, "unknown overlay lane")
    # Old main cannot import candidate-only APIs. If main already committed a
    # candidate test file, its APIs are source-owned; update that test in place
    # under the same four-path exception, but never insert it into old main.
    return {name: data for name, data in overlay.items()
            if lane != "main-default" or name not in CANDIDATE or name in source_manifest}


def overlay_disposition(source_manifest, overlay):
    result = {}
    for name, data in overlay.items():
        prior = source_manifest.get(name)
        new_hash = digest(data)
        if prior is None:
            action = "inserted"
        elif prior["sha256"] == new_hash and prior["mode"] == 0o644:
            action = "reused"
        else:
            require(name in REPLACEABLE_HARNESS, overlay_collision_message(name))
            action = "replaced_test_harness"
        result[name] = {"disposition": action, "prior_sha256": prior["sha256"] if prior else None,
                        "new_sha256": new_hash, "prior_mode": prior["mode"] if prior else None,
                        "new_mode": 0o644, "prior_git_blob": prior["git_blob"] if prior else None}
    return result


def save(path, value):
    with Path(path).open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
    Path(path).chmod(0o600)



def directory_identity(path):
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700,
            f"private directory ownership/mode mismatch: {path}")
    return {"path": str(path), "uid": info.st_uid, "device": info.st_dev,
            "inode": info.st_ino, "mode": stat.S_IMODE(info.st_mode)}


def verify_owned(path, identity):
    require(directory_identity(path) == identity, f"private directory identity changed: {path}")


@contextlib.contextmanager
def short_runtime(output):
    # Never derive capture temp/socket paths from the possibly very long output.
    base = Path("/tmp").resolve(strict=True)
    root = Path(tempfile.mkdtemp(prefix="fc", dir=base))
    identity = directory_identity(root)
    try:
        save(output / "runtime-ownership.json", identity)
        yield root, identity
    finally:
        # Retention avoids path-based deletion races with another same-UID
        # process. Ownership checks are observations, never deletion authority.
        verify_owned(root, identity)
        save(output / "runtime-retention.json", {"status": "retained", "identity": identity,
                                                "manual_cleanup_required": True})
        print("private runtime retained (manual cleanup required): " + str(root), file=sys.stderr)


def runtime_record(private):
    result = {"root": directory_identity(private), "directories": {}}
    for name in ("home", "tmp", "brain", "sockets", "config", "cache"):
        result["directories"][name] = directory_identity(private / name)
    # world.listen joins MkdirTemp("socks") with a decimal PID + .sock.
    # Bound the longest uint32 random suffix and 20-byte PID, below macOS's 104.
    maximum = len(os.fsencode(private / "tmp")) + len("/socks") + 10 + len("/") + 20 + len(".sock")
    require(maximum < 104, "capture runtime path exceeds Unix socket byte budget")
    result["maximum_socket_path_bytes"] = maximum
    return result


def verify_runtime(record):
    verify_owned(Path(record["root"]["path"]), record["root"])
    for identity in record["directories"].values():
        verify_owned(Path(identity["path"]), identity)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--compile-only", action="store_true")
    mode.add_argument("--run", action="store_true")
    parser.add_argument("--baseline", default="origin/main")
    parser.add_argument("--candidate", default="HEAD")
    parser.add_argument("--refresh-main", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args(argv)
    require(args.timeout > 0, "timeout must be positive")
    repo = Path(__file__).resolve().parent.parent
    output = args.output
    require(output.is_absolute() and not output.exists() and not output.is_symlink(), "output must be a NEW absolute directory")
    output = output.resolve()
    worktrees = git(repo, "worktree", "list", "--porcelain", "-z").split(b"\0")
    checkout_roots = [Path(field[len(b"worktree "):].decode()).resolve()
                      for field in worktrees if field.startswith(b"worktree ")]
    require(bool(checkout_roots), "cannot establish primary/worktree output boundaries")
    require(not any(within(output, checkout) for checkout in checkout_roots),
            "output must be outside ALL source checkouts, including primary")
    output.mkdir(mode=0o700)  # exclusive ownership; a failed run remains inspectable, never reused
    check_dirty(repo)
    if args.refresh_main:
        git(repo, "fetch", "origin", "main")
    baseline, candidate = resolve(repo, args.baseline), resolve(repo, args.candidate)
    main_tip = resolve(repo, "origin/main")
    require(git(repo, "merge-base", baseline, main_tip).decode().strip() == baseline,
            "baseline is not a committed ancestor of frozen origin/main")
    source = {}
    for label, sha in (("main", baseline), ("candidate", candidate)):
        source[label] = committed_tree(repo, sha)
    overlay = read_overlay(repo, [source["main"][1], source["candidate"][1]])
    manifest = {
        "schema_version": 2, "mode": "run" if args.run else "compile-only",
        "status": "preparing", "compatibility_pass": False,
        "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "revisions": {"main": baseline, "candidate": candidate},
        "origin_main": main_tip,
        "clock_policy": "fixed fixture clock; only source-enumerated provenance-verified worker/program event.at is tokenized; raw retained",
        "merge_base": git(repo, "merge-base", baseline, candidate).decode().strip(),
        "driver_sha256": file_hash(Path(__file__)),
        "overlay": {k: digest(v) for k, v in overlay.items()},
        "shared_overlay": list(SHARED + FIXTURES), "candidate_only_overlay": list(CANDIDATE),
        "sources": {label: records for label, (_, records) in source.items()},
        "lanes": {}, "commands": [],
    }
    save(output / "provenance.json", manifest)
    with short_runtime(output) as (scratch, scratch_identity):
        manifest["runtime_root"] = scratch_identity
        go = shutil.which("go")
        require(go is not None, "local Go toolchain missing")
        go = str(Path(go).resolve())
        setup = scratch / "setup"
        setup.mkdir(mode=0o700)
        setup_env = private_env(setup)
        setup_env.update(GOENV="off", GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off", GOWORK="off")
        # Go defaults are queried without ambient GOFLAGS, credentials or the user's Go env file.
        # Reuse only an explicit cache or the standard local module cache, never a Go env file.
        ambient_modules = os.environ.get("GOMODCACHE")
        if ambient_modules:
            modules = ambient_modules
        else:
            modules = str(Path.home() / "go/pkg/mod")
        require(Path(modules).is_absolute() and Path(modules).is_dir(), "existing local module cache required (set GOMODCACHE)")
        cache = output / "build-cache"
        cache.mkdir(mode=0o700)
        manifest["versions"] = {"go": bounded([go, "version"], repo, setup_env, args.timeout).decode().strip(),
                                "git": git(repo, "--version").decode().strip(), "python": sys.version}
        manifest["toolchain"] = {"go": go, "sha256": file_hash(go), "module_cache": modules}
        save(output / "toolchain.json", {"versions": manifest["versions"], **manifest["toolchain"]})
        ledger = output / "commands.jsonl"
        ledger.touch(mode=0o600, exist_ok=False)

        def record(command, cwd, env):
            entry = {"args": command, "cwd": str(cwd), "env": dict(env)}
            manifest["commands"].append(entry)
            with ledger.open("a") as stream:
                stream.write(json.dumps(entry, sort_keys=True) + "\n")

        snapshots = {}
        traces, raws = {}, {}
        for lane_index, lane in enumerate(LANES):
            lane_dir = output / lane
            lane_dir.mkdir(mode=0o700)
            label = "main" if lane == "main-default" else "candidate"
            lane_overlay = overlay_for_lane(lane, source[label][1], overlay)
            tree = lane_dir / "source"
            snapshots[lane] = stage(tree, *source[label], lane_overlay)
            disposition = overlay_disposition(source[label][1], lane_overlay)
            save(lane_dir / "overlay-disposition.json", disposition)
            manifest["lanes"][lane] = {"revision": manifest["revisions"][label], "staged": snapshots[lane], "binaries": {},
                                       "overlay_disposition": disposition}
            for package_index, (package, package_dir) in enumerate(PACKAGES.items()):
                private = scratch / f"{lane_index}{package_index}"
                private.mkdir(mode=0o700)
                env = private_env(private, go, cache, modules)
                ownership = runtime_record(private)
                manifest["lanes"][lane].setdefault("runtime", {})[package] = ownership
                save(lane_dir / (package + "-runtime.json"), ownership)
                verify_owned(scratch, scratch_identity)
                verify_runtime(ownership)
                binary = lane_dir / (package + ".test")
                command = [go, "test", "-c", "-o", str(binary), "./" + package_dir]
                record(command, tree, env)
                bounded(command, tree, env, args.timeout, lane_dir / (package + "-compile.log"))
                verify_owned(scratch, scratch_identity)
                verify_runtime(ownership)
                require(binary.is_file() and not binary.is_symlink(), "compiler did not produce test binary")
                manifest["lanes"][lane]["binaries"][package] = file_hash(binary)
                require(tree_manifest(tree) == snapshots[lane], "compilation modified staged source")
                if not args.run:
                    continue
                trace_path = lane_dir / (package + ".json")
                env.update(FACTORY_CLAUDE_COMPAT_CAPTURE="1", FACTORY_CLAUDE_COMPAT_LANE=lane,
                           FACTORY_CLAUDE_COMPAT_TRACE=str(trace_path))
                command = [str(binary), "-test.run=^TestClaudeCompatCapture$", "-test.count=1", "-test.timeout=90s"]
                record(command, tree / package_dir, env)
                bounded(command, tree / package_dir, env, min(args.timeout, 100), lane_dir / (package + "-capture.log"))
                require(file_hash(binary) == manifest["lanes"][lane]["binaries"][package], "runtime changed binary")
                require(tree_manifest(tree) == snapshots[lane], "capture modified pinned source")
                for artifact in (trace_path, Path(str(trace_path) + ".raw.json")):
                    require(artifact.is_file() and not artifact.is_symlink(), "missing/non-regular capture artifact")
                verify_owned(scratch, scratch_identity)
                verify_runtime(ownership)
                raw = load_json(str(trace_path) + ".raw.json")
                trace = load_json(trace_path)
                independent = validate_raw(raw, package, tree, private, binary)
                root_inventory = validate_roots(raw, package, tree, private)
                save(lane_dir / (package + "-root-inventory.json"), root_inventory)
                validate_trace(trace, package)
                diff = first_difference(independent, trace)
                require(not diff, f"{lane}/{package}: emitted normalization differs from raw: {diff}")
                traces[lane, package], raws[lane, package] = trace, raw
                manifest["lanes"][lane].setdefault("artifacts", {})[package] = {
                    "raw_sha256": file_hash(str(trace_path) + ".raw.json"), "trace_sha256": file_hash(trace_path),
                    "root_inventory": root_inventory}
                if lane != "main-default":
                    command = [str(binary), "-test.run=^TestClaudeCompatCandidate", "-test.count=1", "-test.timeout=90s", "-test.v"]
                    record(command, tree / package_dir, env)
                    checks = bounded(command, tree / package_dir, env, min(args.timeout, 100),
                                     lane_dir / (package + "-candidate-checks.log"))
                    verify_owned(scratch, scratch_identity)
                    verify_runtime(ownership)
                    require(re.search(rb"(?m)^--- PASS: TestClaudeCompatCandidate[^ /]*(?: |$)", checks),
                            "candidate-only selection did not execute a passing top-level sentinel")
                    require(tree_manifest(tree) == snapshots[lane], "candidate checks modified pinned source")
                    require(file_hash(binary) == manifest["lanes"][lane]["binaries"][package], "candidate checks changed binary")
                    require(file_hash(trace_path) == manifest["lanes"][lane]["artifacts"][package]["trace_sha256"] and
                            file_hash(str(trace_path) + ".raw.json") == manifest["lanes"][lane]["artifacts"][package]["raw_sha256"],
                            "candidate checks changed shared capture artifacts")
        check_dirty(repo)
        require(read_overlay(repo, [source["main"][1], source["candidate"][1]]) == overlay, "overlay changed during operation")
        if args.run:
            for lane in LANES[1:]:
                for package in PACKAGES:
                    left, right = traces["main-default", package], traces[lane, package]
                    if package == "cli":
                        main_raw, cand_raw = raws["main-default", package], raws[lane, package]
                        compare_cli_inputs(main_raw["cli_inputs"], cand_raw["cli_inputs"], lane == "candidate-explicit",
                                           main_raw["roots"], cand_raw["roots"])
                    diff = first_difference(left, right)
                    require(not diff, f"{lane}/{package}: {diff}")
            manifest["compatibility_pass"] = True
    manifest["runtime_status"] = "retained"
    check_dirty(repo)
    require(read_overlay(repo, [source["main"][1], source["candidate"][1]]) == overlay,
            "overlay changed before final result (including runtime retention checks)")
    require(file_hash(Path(__file__)) == manifest["driver_sha256"], "driver changed during operation")
    manifest["status"] = "captures-compared" if args.run else "compiled-only-NOT-EXECUTED"
    manifest["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save(output / "result.json", manifest)
    print(manifest["status"] + ": " + str(output))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (Invalid, OSError, ValueError) as error:
        print("claude-compat: " + str(error), file=sys.stderr)
        sys.exit(1)
