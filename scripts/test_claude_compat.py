"""Offline driver regression tests. These tests are intentionally NOT RUN at intake."""

import base64
import copy
import hashlib
import importlib.util
import io
import json
import os
import socket
import subprocess
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("claude_compat", Path(__file__).with_name("claude-compat.py"))
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


# Deliberately independent of the driver: changing its contract must fail here.
EXPECTED_INVENTORY = {
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


def evidence():
    return {
        "inputs": ["advance-fixture"],
        "calls": [{"name": "claude", "args": ["launch", "--model", "opus", "--effort", "high"],
                   "dir": "<HOME>/src/r", "env": ["HOME=<HOME>"],
                   "artifacts": {"<BRAIN>/agent.json": '{"role":"planner"}'}}],
        "posts": ["step-end"],
        "events": {"item-1": [{"kind": "worktree-ready", "lease": 7}, {"kind": "coordinator-launch", "lease": 7}]},
        "states": {"item-1": {"owner": "planner", "lease": 7, "head": "abc", "phase": "prepare"}},
        "relations": {"dependent": "item-1->item-2"},
        "assertions": ["observed worktree-ready before coordinator launch"],
    }


def trace(package="cli"):
    return {"schema_version": 1, "package": package, "scenarios": [
        {"name": name, "steps": [{"name": step, "evidence": evidence()} for step in steps]}
        for name, steps in EXPECTED_INVENTORY[package].items()]}


def raw_fixture(package="cli"):
    value = trace(package)
    roots = {"/owned/source": "<SOURCE>"}
    parent = "/owned/private/tmp/TestClaudeCompatCapture123456"
    for index, scenario in enumerate(value["scenarios"]):
        world = index if package == "reconcile" else 0
        brain, home = f"{parent}/{2 * world + 1:03d}", f"{parent}/{2 * world + 2:03d}"
        roots[brain], roots[home] = "<BRAIN>", "<HOME>"
        if package == "reconcile":
            roots[f"/owned/private/tmp/socks{1000 + world}"] = "<SOCKETS>"
        original = json.dumps(scenario).replace("<BRAIN>", brain).replace("<HOME>", home).replace("<SOURCE>", "/owned/source")
        value["scenarios"][index] = json.loads(original)
    inputs = {"intake/defaults": ["add", "a/b#1"], "intake/overrides": ["add", "a/b#2"]} if package == "cli" else {}
    return {"trace": value, "roots": roots, "identities": {}, "cli_inputs": inputs}


class SchemaTests(unittest.TestCase):
    def test_inventory_matches_independent_full_literal(self):
        self.assertEqual(driver.INVENTORY, EXPECTED_INVENTORY)

    def test_removing_reset_from_driver_is_not_blessed_by_fixture(self):
        drifted = copy.deepcopy(EXPECTED_INVENTORY)
        drifted["reconcile"]["usage_policy"] = ("hold", "pause", "stale")
        with patch.object(driver, "INVENTORY", drifted):
            with self.assertRaisesRegex(driver.Invalid, "missing, duplicate or reordered steps"):
                driver.validate_trace(trace("reconcile"), "reconcile")

    def test_both_frozen_inventories_are_accepted(self):
        for package in ("cli", "reconcile"):
            self.assertEqual(driver.validate_trace(trace(package), package), trace(package))
        self.assertEqual(len(driver.INVENTORY["reconcile"]), 9)

    def test_envelope_tampering_rejected(self):
        mutations = [
            lambda t: t.update(extra=True),
            lambda t: t.update(schema_version=True),
            lambda t: t.update(schema_version=2),
            lambda t: t.update(package="reconcile"),
            lambda t: t.update(scenarios=[]),
            lambda t: t["scenarios"].append(copy.deepcopy(t["scenarios"][0])),
            lambda t: t["scenarios"][0].update(extra=True),
            lambda t: t["scenarios"][0].update(name="unknown"),
            lambda t: t["scenarios"][0].update(steps=[]),
            lambda t: t["scenarios"][0]["steps"].reverse(),
            lambda t: t["scenarios"][0]["steps"][0].update(extra=True),
            lambda t: t["scenarios"][0]["steps"][0].pop("evidence"),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                value = trace()
                mutation(value)
                with self.assertRaises(driver.Invalid):
                    driver.validate_trace(value, "cli")

    def test_nested_types_and_unknown_envelopes_rejected(self):
        mutations = [
            lambda e: e.update(extra="unknown"),
            lambda e: e.update(inputs="advance"),
            lambda e: e.update(assertions=[]),
            lambda e: e.update(posts=[1]),
            lambda e: e.update(calls={}),
            lambda e: e["calls"][0].update(extra=True),
            lambda e: e["calls"][0].update(name=1),
            lambda e: e["calls"][0].update(args=[{}]),
            lambda e: e["calls"][0].update(env={}),
            lambda e: e["calls"][0].update(dir=[]),
            lambda e: e["calls"][0].update(artifacts={"path": 1}),
            lambda e: e.update(events={"item": ["not an official object"]}),
            lambda e: e.update(events={"item": [{}]}),
            lambda e: e.update(states={"item": []}),
            lambda e: e.update(states={"item": {}}),
            lambda e: e.update(relations={"dependency": []}),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                value = trace()
                mutation(value["scenarios"][0]["steps"][0]["evidence"])
                with self.assertRaises(driver.Invalid):
                    driver.validate_trace(value, "cli")

    def test_equally_empty_traces_cannot_pass(self):
        value = trace()
        for step in value["scenarios"][0]["steps"]:
            e = step["evidence"]
            e.update(calls=[], posts=[], events={}, states={}, relations={})
        with self.assertRaisesRegex(driver.Invalid, "empty evidence"):
            driver.validate_trace(value, "cli")

    def test_assertions_alone_do_not_prove_operations(self):
        value = trace()
        for step in value["scenarios"][0]["steps"]:
            step["evidence"].update(calls=[], posts=[], events={})
        with self.assertRaisesRegex(driver.Invalid, "no observed operations"):
            driver.validate_trace(value, "cli")

    def test_new_official_record_fields_are_preserved(self):
        value = trace()
        value["scenarios"][0]["steps"][0]["evidence"]["states"]["item-1"]["new_field"] = {"value": 9}
        driver.validate_trace(value, "cli")
        self.assertIsNotNone(driver.first_difference(trace(), value))

    def test_personal_repo_review_requests_fail_in_every_lane(self):
        forbidden = [
            ["pr", "create", "--reviewer", "alice"], ["pr", "create", "--reviewer=alice"],
            ["pr", "create", "-r", "alice"], ["pr", "create", "-ralice"], ["pr", "create", "-r=alice"],
            ["pr", "edit", "--add-reviewer", "alice"], ["pr", "edit", "--add-reviewer=alice"],
            ["api", "-X", "POST", "repos/kingpinXD/factory/pulls/1/requested_reviewers"],
            ["api", "--method=POST", "repos/kingpinXD/factory/pulls/1/requested_reviewers"],
            ["api", "-XPOST", "repos/kingpinXD/factory/pulls/1/requested_reviewers"],
            ["api", "repos/kingpinXD/factory/pulls/1/requested_reviewers", "-f", "reviewers[]=alice"],
            ["api", "repos/kingpinXD/factory/pulls/1/requested_reviewers", "--input=reviewers.json"],
            ["api", "graphql", "-f", "query=mutation { requestReviews(input:{pullRequestId:\"p\"}) {clientMutationId} }"],
        ]
        for args in forbidden:
            value = trace()
            call = value["scenarios"][0]["steps"][0]["evidence"]["calls"][0]
            call.update(name="gh", args=args)
            with self.subTest(args=args), self.assertRaisesRegex(driver.Invalid, "review request forbidden"):
                driver.validate_trace(value, "cli")

    def test_review_authoring_and_review_request_reads_are_not_assignment(self):
        permitted = [
            ["pr", "create", "--title", "Example"], ["pr", "review", "--approve"],
            ["pr", "review", "--request-changes"],
            ["api", "-X", "POST", "repos/kingpinXD/factory/pulls/1/reviews", "-f", "event=APPROVE"],
            ["api", "graphql", "-f", "query=mutation { addPullRequestReview(input:{pullRequestId:\"p\"}) {clientMutationId} }"],
            ["api", "repos/kingpinXD/factory/pulls/1/requested_reviewers"],
            ["api", "--method", "GET", "repos/kingpinXD/factory/pulls/1/requested_reviewers"],
            ["api", "-X", "DELETE", "repos/kingpinXD/factory/pulls/1/requested_reviewers"],
        ]
        for args in permitted:
            with self.subTest(args=args):
                driver.no_review_request({"name": "gh", "args": args}, "test")


class ComparisonTests(unittest.TestCase):
    def test_model_effort_role_owner_lease_ref_phase_and_dependency_drift(self):
        mutations = [
            lambda e: e["calls"][0]["args"].__setitem__(2, "sonnet"),
            lambda e: e["calls"][0]["args"].__setitem__(4, "low"),
            lambda e: e["calls"][0]["artifacts"].update({"<BRAIN>/agent.json": '{"role":"reviewer"}'}),
            lambda e: e["states"]["item-1"].update(owner="babysitter"),
            lambda e: e["states"]["item-1"].update(lease=8),
            lambda e: e["states"]["item-1"].update(head="def"),
            lambda e: e["states"]["item-1"].update(phase="merge"),
            lambda e: e["relations"].update(dependent="item-2->item-1"),
            lambda e: e["events"]["item-1"].reverse(),
            lambda e: e["posts"].append("out-of-order"),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                value = trace()
                mutation(value["scenarios"][0]["steps"][0]["evidence"])
                self.assertIsNotNone(driver.first_difference(trace(), value))

    def test_calls_and_action_order_are_not_sorted(self):
        original = trace()
        calls = original["scenarios"][0]["steps"][0]["evidence"]["calls"]
        calls.append({"name": "git", "args": ["worktree", "add"], "dir": "<SOURCE>", "env": [], "artifacts": {}})
        value = copy.deepcopy(original)
        value["scenarios"][0]["steps"][0]["evidence"]["calls"].reverse()
        self.assertIn("calls[0]", driver.first_difference(original, value))

    def test_map_order_ignored_but_array_order_retained(self):
        self.assertIsNone(driver.first_difference({"a": 1, "b": 2}, {"b": 2, "a": 1}))
        self.assertIsNotNone(driver.first_difference([1, 2], [2, 1]))

    def test_cli_only_one_exact_explicit_pair_may_differ(self):
        main = {"intake/defaults": ["add", "owner/repo#1"], "intake/overrides": ["add", "owner/repo#2", "--model", "opus"]}
        default = copy.deepcopy(main)
        driver.compare_cli_inputs(main, default, False, {}, {})
        explicit = {k: v + ["--executor", "claude"] for k, v in main.items()}
        driver.compare_cli_inputs(main, explicit, True, {}, {})
        bad_values = [main, {k: v + ["--executor", "native"] for k, v in main.items()},
                      {k: v + ["--executor=claude"] for k, v in main.items()},
                      {k: v + ["--executor", "claude", "--executor", "claude"] for k, v in main.items()},
                      {k: v + ["--executor", "claude", "--model", "sonnet"] for k, v in main.items()}]
        for value in bad_values:
            with self.subTest(value=value), self.assertRaises(driver.Invalid):
                driver.compare_cli_inputs(main, value, True, {}, {})
        with self.assertRaises(driver.Invalid):
            driver.compare_cli_inputs(main, explicit, False, {}, {})

    def test_actual_program_executor_args_are_never_stripped(self):
        left = trace()
        right = copy.deepcopy(left)
        right["scenarios"][0]["steps"][0]["evidence"]["calls"][0]["args"] += ["--executor", "claude"]
        self.assertIsNotNone(driver.first_difference(left, right))


class RawTests(unittest.TestCase):
    def raw(self):
        return raw_fixture()

    def test_owned_roots_and_path_relationships(self):
        value = {"/owned/private/home/a": ["HOME=/owned/private/home", "/owned/private/home/b", "/owned/private/homebrew"]}
        expected = {"<HOME>/a": ["HOME=<HOME>", "<HOME>/b", "/owned/private/homebrew"]}
        self.assertEqual(driver.normalize(value, {"/owned/private/home": "<HOME>"}), expected)
        self.assertEqual(driver.normalize("/elsewhere/owned/private/home/a", {"/owned/private/home": "<HOME>"}),
                         "/elsewhere/owned/private/home/a")
        raw = self.raw()
        driver.validate_raw(raw, "cli", Path("/owned/source"), Path("/owned/private"), Path("/owned/cli.test"))

    def test_independent_normalization_catches_emitted_tampering(self):
        raw = self.raw()
        raw["trace"]["scenarios"][0]["steps"][0]["evidence"]["calls"][0]["dir"] = "/owned/source/repo"
        independent = driver.validate_raw(raw, "cli", Path("/owned/source"), Path("/owned/private"), Path("/owned/cli.test"))
        emitted = copy.deepcopy(independent)
        emitted["scenarios"][0]["steps"][0]["evidence"]["states"]["item-1"]["lease"] = 0
        self.assertIsNotNone(driver.first_difference(independent, emitted))

    def test_masks_outside_owned_roots_and_identity_masks_rejected(self):
        mutations = [lambda r: r.update(identities={"opus": "model"}),
                     lambda r: r.update(roots={"/": "<HOME>"}),
                     lambda r: r.update(roots={"/owned/source": "<BRAIN>"}),
                     lambda r: r.update(roots={"/owned/private/home": "<LEASE>"}),
                     lambda r: r.update(roots={"/owned/private/../else": "<HOME>"}),
                     lambda r: r.update(cli_inputs={}),
                     lambda r: r.update(extra="unknown"),
                     lambda r: r.pop("trace"),
                     lambda r: r.update(trace=trace())]
        for mutation in mutations:
            value = self.raw()
            mutation(value)
            with self.subTest(mutation=mutation), self.assertRaises(driver.Invalid):
                driver.validate_raw(value, "cli", Path("/owned/source"), Path("/owned/private"), Path("/owned/cli.test"))

    def test_normalization_key_collision_fails_closed(self):
        with self.assertRaisesRegex(driver.Invalid, "collapse"):
            driver.normalize({"/one/a": 1, "/two/a": 2}, {"/one": "<HOME>", "/two": "<HOME>"})

    def test_duplicate_keys_and_nonfinite_numbers_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            p = Path(temp) / "trace.json"
            for value in ('{"a":1,"a":2}', '{"a":NaN}', '{"a":Infinity}', '{"a":1e999}'):
                p.write_text(value)
                with self.assertRaises(driver.Invalid):
                    driver.load_json(p)


class RootInventoryTests(unittest.TestCase):
    def validate(self, raw, package="reconcile"):
        return driver.validate_raw(raw, package, Path("/owned/source"), Path("/owned/private"), Path("/owned/reconcile.test"))

    def test_actual_newworld_and_cli_maker_layouts_accepted(self):
        for package in ("reconcile", "cli"):
            with self.subTest(package=package):
                self.validate(raw_fixture(package), package)

    def test_clone_child_and_nested_shadow_cannot_be_promoted_to_home(self):
        for suffix in ("/repo-A", "/src/r", "/src/s", "/nested/repo-B"):
            raw = raw_fixture()
            home = next(k for k, v in raw["roots"].items() if v == "<HOME>")
            del raw["roots"][home]
            raw["roots"][home + suffix] = "<HOME>"
            with self.subTest(suffix=suffix), self.assertRaises(driver.Invalid):
                self.validate(raw, "cli")
        raw = raw_fixture()
        home = next(k for k, v in raw["roots"].items() if v == "<HOME>")
        raw["roots"][home + "/src/r"] = "<HOME>"
        raw["roots"][home + "/src/s"] = "<HOME>"
        with self.assertRaises(driver.Invalid):
            self.validate(raw, "cli")

    def test_wrong_maker_parent_counts_roles_socket_shape_and_extra_tokens_rejected(self):
        mutations = [
            lambda roots: roots.update({"/owned/private/home": "<HOME>"}),
            lambda roots: roots.update({"/owned/private/tmp/repo-A": "<WORKSPACE>"}),
            lambda roots: roots.update({"/owned/private/tmp/socks1000/child": "<SOCKETS>"}),
            lambda roots: roots.pop("/owned/private/tmp/socks1000"),
            lambda roots: roots.update({"/owned/private/tmp/TestClaudeCompatCapture123456/001": "<HOME>"}),
            lambda roots: roots.update({"/owned/private/tmp/TestClaudeCompatCapture987654/002": "<HOME>"}),
            lambda roots: roots.update({"/owned/source/subdir": "<SOURCE>"}),
        ]
        for mutation in mutations:
            raw = raw_fixture("reconcile")
            mutation(raw["roots"])
            with self.subTest(mutation=mutation), self.assertRaises(driver.Invalid):
                self.validate(raw)

    def test_root_maps_must_match_scenario_creation_order(self):
        raw = raw_fixture("reconcile")
        first, second = raw["trace"]["scenarios"][:2]
        first["steps"][0]["evidence"]["relations"]["wrong_home"] = "/owned/private/tmp/TestClaudeCompatCapture123456/004/src/r"
        with self.assertRaisesRegex(driver.Invalid, "another fixture world's root"):
            self.validate(raw)

    def test_repository_suffix_and_json_response_evidence_remain_visible(self):
        raw = raw_fixture()
        home = next(k for k, v in raw["roots"].items() if v == "<HOME>")
        e = raw["trace"]["scenarios"][0]["steps"][0]["evidence"]
        response = '{"call_index":0,"tick":1,"kind":"worktree","repo":"o/r","pr":0,"sha":"head-1","stdout":"' + home + '/src/r-worktrees/factory-e1","error":""}'
        e["relations"]["read_call:0"] = response
        normalized = self.validate(raw, "cli")
        got = normalized["scenarios"][0]["steps"][0]["evidence"]["relations"]["read_call:0"]
        self.assertEqual(got, '{"call_index":0,"tick":1,"kind":"worktree","repo":"o/r","pr":0,"sha":"head-1","stdout":"<HOME>/src/r-worktrees/factory-e1","error":""}')
        changed = copy.deepcopy(raw)
        changed["trace"]["scenarios"][0]["steps"][0]["evidence"]["relations"]["read_call:0"] = response.replace("/src/r-worktrees/", "/src/s-worktrees/")
        self.assertIsNotNone(driver.first_difference(normalized, self.validate(changed, "cli")))


class RuntimeOwnershipTests(unittest.TestCase):
    def test_long_output_cannot_lengthen_private_socket_path_and_runtime_is_retained(self):
        with tempfile.TemporaryDirectory(dir=Path("/tmp").resolve(), prefix="fc-test-") as temp:
            fixture = Path(temp)
            output = fixture / ("a" * 90) / ("b" * 90) / ("c" * 90)
            output.mkdir(parents=True)
            scratch = fixture / "r"
            scratch.mkdir(mode=0o700)
            # The enclosing test fixture owns eventual test teardown. The driver
            # itself must never call a recursive remover or a cleanup subprocess.
            with patch.object(driver.tempfile, "mkdtemp", return_value=str(scratch)), \
                 patch.object(driver.shutil, "rmtree") as delete, patch.object(driver, "bounded") as commands:
                with driver.short_runtime(output) as (root, identity):
                    private = root / "00"
                    private.mkdir(mode=0o700)
                    env = driver.private_env(private)
                    record = driver.runtime_record(private)
                    self.assertLess(record["maximum_socket_path_bytes"], 104)
                    self.assertNotIn(str(output), env["TMPDIR"])
                    self.assertNotEqual(env["HOME"], env["TMPDIR"])
                    socks = Path(env["TMPDIR"]) / "socks1234567890"
                    socks.mkdir(mode=0o700)
                    endpoint = socks / ("9" * 20 + ".sock")
                    with socket.socket(socket.AF_UNIX) as connection:
                        connection.bind(str(endpoint))
                    driver.verify_owned(root, identity)
                    driver.verify_runtime(record)
                driver.verify_owned(root, identity)
                retained = json.loads((output / "runtime-retention.json").read_text())
                self.assertEqual(retained["status"], "retained")
                self.assertEqual(retained["identity"], identity)
                self.assertTrue(retained["manual_cleanup_required"])
                self.assertFalse((output / "runtime-cleanup.json").exists())
                delete.assert_not_called()
                commands.assert_not_called()

    def test_ownership_identity_mode_and_symlinks_fail_closed(self):
        with tempfile.TemporaryDirectory(dir=Path("/tmp").resolve(), prefix="fc-test-") as temp:
            output = Path(temp)
            scratch = output / "r"
            scratch.mkdir(mode=0o700)
            with patch.object(driver.tempfile, "mkdtemp", return_value=str(scratch)):
                with driver.short_runtime(output) as (root, identity):
                    other = root / "other"
                    other.mkdir(mode=0o700)
                    with self.assertRaisesRegex(driver.Invalid, "identity changed"):
                        driver.verify_owned(other, identity)
                    link = root / "link"
                    link.symlink_to(other, target_is_directory=True)
                    with self.assertRaisesRegex(driver.Invalid, "ownership/mode"):
                        driver.directory_identity(link)
                    root.chmod(0o755)
                    try:
                        with self.assertRaisesRegex(driver.Invalid, "ownership/mode"):
                            driver.verify_owned(root, identity)
                    finally:
                        root.chmod(0o700)
                self.assertTrue(root.exists())

    def test_same_uid_directory_replacement_is_never_deleted(self):
        with tempfile.TemporaryDirectory(dir=Path("/tmp").resolve(), prefix="fc-test-") as temp:
            output = Path(temp)
            scratch, moved = output / "r", output / "original"
            scratch.mkdir(mode=0o700)
            with patch.object(driver.tempfile, "mkdtemp", return_value=str(scratch)), \
                 patch.object(driver.shutil, "rmtree") as delete, patch.object(driver, "bounded") as commands:
                with self.assertRaisesRegex(driver.Invalid, "identity changed"):
                    with driver.short_runtime(output) as (root, identity):
                        root.rename(moved)
                        root.mkdir(mode=0o700)
                        (root / "foreign-data").write_text("retain this replacement")
                self.assertEqual((scratch / "foreign-data").read_text(), "retain this replacement")
                self.assertTrue(moved.is_dir())
                delete.assert_not_called()
                commands.assert_not_called()


class WorkerTimeTests(unittest.TestCase):
    def raw(self):
        value = raw_fixture("reconcile")
        e = self.evidence(value)
        at = "2026-10-18T12:00:00.123456789Z"
        e["events"] = {"item-1": [
            {"seq": 11, "at": "2026-10-18T11:59:00Z", "kind": "request", "sender": "factory",
             "recipient": "repo-worker", "request": "worktree", "key": "repo:worktree:5"},
            {"seq": 12, "at": at, "kind": "worktree_ready", "sender": "factory",
             "trigger_ref": "11", "key": "repo:11", "text": at, "lease": 7}]}
        e["states"]["item-1"]["observed_at"] = at
        e["posts"].append(at)
        value["identities"] = {"worker_event_times": [
            {"scenario": "worker_failures", "step": "prepare_retry", "entity": "item-1", "seq": 12, "at": at}]}
        return value

    def evidence(self, raw):
        return next(step["evidence"] for scenario in raw["trace"]["scenarios"] if scenario["name"] == "worker_failures"
                    for step in scenario["steps"] if step["name"] == "prepare_retry")

    def normalize(self, raw):
        return driver.validate_raw(raw, "reconcile", Path("/owned/source"), Path("/owned/private"), Path("/owned/reconcile.test"))

    def test_only_verified_result_at_changes_and_original_is_retained(self):
        raw = self.raw()
        original = copy.deepcopy(raw)
        normalized = self.normalize(raw)
        e = self.evidence({"trace": normalized})
        result = e["events"]["item-1"][1]
        self.assertEqual(result["at"], "<WORKER_AT:item-1:12>")
        self.assertEqual(result["text"], "2026-10-18T12:00:00.123456789Z")
        self.assertEqual(result["lease"], 7)
        self.assertEqual(e["states"]["item-1"]["observed_at"], result["text"])
        self.assertEqual(e["posts"][-1], result["text"])
        self.assertEqual(e["events"]["item-1"][0]["at"], "2026-10-18T11:59:00Z")
        self.assertEqual(raw, original)

    def test_two_actual_result_times_normalize_but_unrelated_clock_drift_does_not(self):
        raw, changed = self.raw(), self.raw()
        at = "2026-10-18T12:01:00.000000001Z"
        self.evidence(changed)["events"]["item-1"][1]["at"] = at
        changed["identities"]["worker_event_times"][0]["at"] = at
        self.assertEqual(self.normalize(raw), self.normalize(changed))
        self.evidence(changed)["states"]["item-1"]["observed_at"] = at
        self.assertIsNotNone(driver.first_difference(self.normalize(raw), self.normalize(changed)))

    def test_worktree_cleanup_and_error_results_keep_request_linkage(self):
        for operation, kind in (("worktree", "worktree_ready"), ("cleanup", "done"),
                                ("worktree", "error"), ("cleanup", "error")):
            value = self.raw()
            events = self.evidence(value)["events"]["item-1"]
            events[0]["request"], events[1]["kind"] = operation, kind
            with self.subTest(operation=operation, kind=kind):
                self.normalize(value)

    def test_tampered_results_rejected(self):
        mutations = [lambda e: e.update(sender="planner"), lambda e: e.update(kind="transition"),
                     lambda e: e.update(kind=[]), lambda e: e.update(key="repo:10"),
                     lambda e: e.update(trigger_ref="10"), lambda e: e.update(trigger_ref=11),
                     lambda e: e.update(trigger_ref="011", key="repo:011"),
                     lambda e: e.update(seq=True), lambda e: e.update(seq="12"),
                     lambda e: e.update(seq=11), lambda e: e.pop("at"),
                     lambda e: e.update(at="2026-10-18T11:58:59Z"),
                     lambda e: e.update(at="not-a-time"), lambda e: e.update(at=123)]
        for mutate in mutations:
            value = self.raw()
            mutate(self.evidence(value)["events"]["item-1"][1])
            with self.subTest(mutation=mutate), self.assertRaises(driver.Invalid):
                self.normalize(value)

    def test_tampered_requests_rejected(self):
        mutations = [lambda e: e.update(kind="message"), lambda e: e.update(recipient="planner"),
                     lambda e: e.update(request="merge"), lambda e: e.update(request=[]),
                     lambda e: e.update(request="cleanup"), lambda e: e.update(seq=10),
                     lambda e: e.update(seq=True), lambda e: e.update(at="2026-10-18T12:01:00Z"),
                     lambda e: e.update(at=None), lambda e: e.pop("at")]
        for mutate in mutations:
            value = self.raw()
            mutate(self.evidence(value)["events"]["item-1"][0])
            with self.subTest(mutation=mutate), self.assertRaises(driver.Invalid):
                self.normalize(value)
        for mutation in (lambda events: events.reverse(), lambda events: events.pop(0),
                         lambda events: events.insert(0, copy.deepcopy(events[0])),
                         lambda events: events.append(copy.deepcopy(events[-1]))):
            value = self.raw()
            mutation(self.evidence(value)["events"]["item-1"])
            with self.subTest(mutation=mutation), self.assertRaises(driver.Invalid):
                self.normalize(value)

    def test_missing_duplicate_absent_wrong_type_and_unknown_identity_mappings(self):
        mutations = [lambda r: r.update(identities={}), lambda r: r.update(identities=[]),
                     lambda r: r.update(identities={"worker_event_times": []}),
                     lambda r: r.update(identities={"worker_event_times": {}}),
                     lambda r: r["identities"].update(other=[]),
                     lambda r: r["identities"]["worker_event_times"][0].update(token="<WORKER_AT:item-1:12>"),
                     lambda r: r["identities"]["worker_event_times"].append(copy.deepcopy(r["identities"]["worker_event_times"][0])),
                     lambda r: r["identities"]["worker_event_times"].append(None)]
        for mutation in mutations:
            value = self.raw()
            mutation(value)
            with self.subTest(mutation=mutation), self.assertRaises(driver.Invalid):
                self.normalize(value)
        for key, wrong in (("scenario", "restart"), ("step", "cleanup"), ("entity", "item-2"),
                           ("seq", 99), ("seq", True), ("seq", 12.0), ("at", "2026-10-18T12:01:00Z"),
                           ("at", None), ("entity", [])):
            value = self.raw()
            value["identities"]["worker_event_times"][0][key] = wrong
            with self.subTest(key=key, wrong=wrong), self.assertRaises(driver.Invalid):
                self.normalize(value)
        for key in ("scenario", "step", "entity", "seq", "at"):
            value = self.raw()
            del value["identities"]["worker_event_times"][0][key]
            with self.subTest(missing=key), self.assertRaises(driver.Invalid):
                self.normalize(value)

    def test_mapping_is_per_snapshot_not_global_entity_sequence(self):
        value = self.raw()
        scenario = next(s for s in value["trace"]["scenarios"] if s["name"] == "worker_failures")
        scenario["steps"][2]["evidence"]["events"] = copy.deepcopy(self.evidence(value)["events"])
        with self.assertRaisesRegex(driver.Invalid, "missing worker timestamp"):
            self.normalize(value)
        entry = copy.deepcopy(value["identities"]["worker_event_times"][0])
        entry["step"] = "cleanup"
        value["identities"]["worker_event_times"].append(entry)
        self.normalize(value)

    def test_raw_tokens_rejected_and_emitted_token_tampering_detected(self):
        value = self.raw()
        self.evidence(value)["events"]["item-1"][1]["at"] = "<WORKER_AT:item-1:12>"
        with self.assertRaisesRegex(driver.Invalid, "already contains normalization tokens"):
            self.normalize(value)
        normalized = self.normalize(self.raw())
        emitted = copy.deepcopy(normalized)
        self.evidence({"trace": emitted})["events"]["item-1"][1]["at"] = "<WORKER_AT:item-2:12>"
        self.assertIsNotNone(driver.first_difference(normalized, emitted))

    def test_nanosecond_order_and_offsets_are_checked_without_truncation(self):
        value = self.raw()
        events = self.evidence(value)["events"]["item-1"]
        events[0]["at"] = "2026-10-18T12:00:00.123456790Z"
        with self.assertRaisesRegex(driver.Invalid, "precedes request timestamp"):
            self.normalize(value)
        events[0]["at"] = "2026-10-18T13:00:00.123456789+01:00"
        self.normalize(value)
        for timestamp in ("2026-10-18", "2026-10-18T12:00:00", "2026-02-30T12:00:00Z",
                          "2026-10-18T12:00:00+01:99", "2026-10-18T12:00:00.Z"):
            with self.subTest(timestamp=timestamp), self.assertRaises(driver.Invalid):
                driver.event_time(timestamp, "test")


PROGRAM_FAMILIES = {"checked_plan", "mirror", "assignment", "handover", "cancel_done", "stop_propagation",
                    "cancel_cleanup", "cancel_unassign", "stop_refused", "cancel_message"}


class ProgramTimeTests(unittest.TestCase):
    def fixture(self, family, already=False, cancel_kind="work"):
        raw = raw_fixture("reconcile")
        scenario = next(s for s in raw["trace"]["scenarios"] if s["name"] == "checkpoint")
        e = scenario["steps"][0]["evidence"]
        brain = str(Path(next(iter(e["calls"][0]["artifacts"]))).parent)
        fixed, actual = "2026-10-09T12:00:00Z", "2026-10-19T03:07:26.123456789Z"
        e["events"], e["states"] = {}, {}
        entries = []
        raw["identities"] = {"program_event_times": entries}

        def entity(name, kind, extra="", owner="e1"):
            directory = brain + "/entities/" + name
            e["relations"]["index:" + name] = kind + ":" + ("" if kind == "epic" else owner) + ":" + directory
            e["relations"]["inputs:" + name] = "id: " + name + "\n" + extra
            e["events"][name] = []
            e["states"][name] = {"state": "cancelled", "lease": 7, "head": "head-1", "observed_at": actual}
            return directory

        def event(name, kind, **fields):
            record = {"seq": len(e["events"][name]) + 1, "at": fixed, "kind": kind, "sender": "fixture-agent", **fields}
            e["events"][name].append(record)
            return record

        def program(name, family, kind, **fields):
            record = event(name, kind, sender="factory", at=actual, **fields)
            entries.append({"scenario": "checkpoint", "step": "open_step", "entity": name,
                            "seq": record["seq"], "at": actual, "family": family})
            e["relations"][f"event_tick:{name}#{record['seq']}"] = "1"
            return record

        def cancelled(name):
            request = event(name, "request", sender="user", request="stop", to="cancelled", trigger="user", text="reason")
            transition = event(name, "transition", sender="factory", to="cancelled", trigger="user", trigger_ref=str(request["seq"]))
            return transition["seq"]

        def assignment(name):
            event(name, "transition", sender="factory", to="starting")
            call_index = len(e["calls"])
            for args in (["api", "user"], ["api", "repos/o/r/issues/12"]):
                e["calls"].append({"name": "gh", "args": args, "dir": "", "env": [], "artifacts": {}})
            for offset, kind, repo, number, response in (
                    (0, "me", "", "", {"login": "actual-user"}),
                    (1, "issue", "o/r", "12", {"number": 12, "repository_url": "https://api.github.com/repos/o/r",
                     "assignees": [{"login": "actual-user"}] if already else []})):
                index = call_index + offset
                e["relations"][f"read_call:{index}"] = json.dumps({"call_index": index, "tick": 1, "kind": kind,
                        "repo": repo, "pr": number, "sha": "", "stdout": json.dumps(response), "error": ""})
            if not already:
                e["calls"].append({"name": "gh", "args": ["issue", "edit", "12", "-R", "o/r", "--add-assignee", "@me"],
                                   "dir": "", "env": [], "artifacts": {}})
            return program(name, "assignment", "done", key="assign:" + name, text="already assigned" if already else "assigned")

        work = "e1-w1"
        work_inputs = "epic: e1\nset: e1-s1\nrepo: o/r\nissue: 12\n"
        if family in {"checked_plan", "mirror"}:
            name, kind = "e1", "epic"
            directory = entity(name, kind)
            body = ("uat: none\nissues:\n  - {ref: o/r#12, result: ready}\n  - {ref: o/r#13, result: ready}\n"
                    "items:\n  - {id: w1, repo: r, issue: o/r#12}\n  - {id: w2, repo: r, issue: o/r#13}\n"
                    "sets:\n  - {id: s1, items: [w1]}\n  - {id: s2, items: [w2]}\n"
                    "links:\n  - {from: w1, to: w2, gate: start, when: merged}\n")
            end = event(name, "end", file=directory + "/epic.v1.yaml", sha256=hashlib.sha256(body.encode()).hexdigest())
            e["relations"][f"plan_content:{name}#{end['seq']}"] = body
            target = program(name, "checked_plan", "done", key="checked-plan:1", text="checked epic.v1.yaml")
            if family == "mirror":
                event(name, "transition", sender="factory", to="planned", trigger_ref="1")
                target = program(name, family, "done", key="mirror:1", text="mirror 1 blocked-by links of e1 to GitHub")
        elif family == "assignment":
            name = work
            entity(name, "work", work_inputs)
            target = assignment(name)
        elif family == "handover":
            name = work
            directory = entity(name, "work", work_inputs)
            event(name, "transition", sender="factory", to="starting")
            event(name, "transition", sender="factory", to="pr_open")
            event(name, "end", file=directory + "/pr.md")
            target = program(name, family, "handover", key="handover:2", text="the pr-opener ended")
        elif family in {"cancel_done", "cancel_cleanup", "cancel_unassign"}:
            name = "e1" if cancel_kind == "epic" else "e1-s1" if cancel_kind == "set" else work
            entity(name, cancel_kind, work_inputs)
            if family == "cancel_cleanup":
                event(name, "request", recipient="repo-worker", request="worktree")
            if family == "cancel_unassign":
                assignment(name)
            ref = cancelled(name)
            if family == "cancel_done":
                target = program(name, family, "done", key=f"cancel:{ref}", text="cancelled: factory stop: reason")
            elif family == "cancel_cleanup":
                target = program(name, family, "request", key=f"repo:cleanup:cancel:{ref}", recipient="repo-worker", request="cleanup", text="cancelled")
            else:
                e["calls"].append({"name": "gh", "args": ["issue", "edit", "12", "-R", "o/r", "--remove-assignee", "@me"],
                                   "dir": "", "env": [], "artifacts": {}})
                target = program(name, family, "done", key=f"cancel:{ref}:unassign", text="unassign o/r#12")
        elif family == "stop_propagation":
            name = work
            entity(name, "work", work_inputs)
            entity("e1-s1", "set", "epic: e1\nitems:\n    - e1-w1\n")
            ref = cancelled("e1-s1")
            target = program(name, family, "request", key=f"stop:e1-s1:{ref}", request="stop", to="cancelled", trigger="user", text="reason")
        elif family == "stop_refused":
            name = work
            entity(name, "work", work_inputs)
            cancelled(name)
            request = event(name, "request", sender="user", request="stop", to="cancelled", trigger="user", text="reason")
            target = program(name, family, "refused", key=f"refused:{request['seq']}", trigger_ref=str(request["seq"]),
                             trigger="user", to="cancelled", text="cancelled → cancelled on user: not an allowed move", **{"from": "cancelled"})
        else:
            name = "e1-s1"
            entity(name, "set", "epic: e1\nitems:\n    - e1-w1\n")
            entity(work, "work", work_inputs)
            ref = cancelled(work)
            entity(name + "-orchestrator", "session", "task: e1-s1\ncomponent: orchestrator\n")
            event(name + "-orchestrator", "start")
            target = program(name, family, "message", key=f"cancel:{ref}:tell",
                             text="e1-w1 is cancelled (factory stop: reason): stop work on it; the program removes its worktree.")
        e["relations"]["tick_calls:1"] = json.dumps({"start": 0, "end": len(e["calls"])})
        return raw, e, name, target

    def normalize(self, raw):
        return driver.validate_raw(raw, "reconcile", Path("/owned/source"), Path("/owned/private"), Path("/owned/reconcile.test"))

    def test_all_ten_families_change_only_their_at_fields_and_preserve_original(self):
        self.assertEqual(set(driver.PROGRAM_EVENTS), PROGRAM_FAMILIES)
        for family in sorted(PROGRAM_FAMILIES):
            with self.subTest(family=family):
                raw, e, name, target = self.fixture(family)
                original = copy.deepcopy(raw)
                actual = driver.normalize_event_times(raw["trace"], raw["identities"])
                expected = copy.deepcopy(raw["trace"])
                steps = next(s for s in expected["scenarios"] if s["name"] == "checkpoint")["steps"]
                for entry in raw["identities"]["program_event_times"]:
                    steps[0]["evidence"]["events"][entry["entity"]][entry["seq"] - 1]["at"] = f"<PROGRAM_AT:{entry['entity']}:{entry['seq']}>"
                self.assertEqual(actual, expected)
                self.assertEqual(raw, original)
                self.normalize(raw)
        for kind in ("epic", "set"):
            with self.subTest(cancel_kind=kind):
                self.normalize(self.fixture("cancel_done", cancel_kind=kind)[0])
        self.normalize(self.fixture("assignment", already=True)[0])

    def test_tampered_mapping_schema_and_missing_duplicate_or_unsupported_entries(self):
        mutations = [lambda r: r.update(identities={}), lambda r: r["identities"].update(program_event_times=[]),
                     lambda r: r["identities"].update(program_event_times={}), lambda r: r["identities"].update(other_times=[]),
                     lambda r: r["identities"]["program_event_times"].append(copy.deepcopy(r["identities"]["program_event_times"][0]))]
        for mutation in mutations:
            raw = self.fixture("assignment")[0]
            mutation(raw)
            with self.subTest(mutation=mutation), self.assertRaises(driver.Invalid):
                self.normalize(raw)
        for key, value in (("family", "unknown"), ("family", "handover"), ("family", []), ("seq", True), ("seq", 2.0),
                           ("seq", 99), ("entity", "another-work"), ("scenario", "unknown"), ("step", "unknown"),
                           ("at", "2026-10-19T04:00:00Z"), ("at", None), ("extra", "discard")):
            raw = self.fixture("assignment")[0]
            raw["identities"]["program_event_times"][0][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(driver.Invalid):
                self.normalize(raw)
        for key in ("scenario", "step", "entity", "seq", "at", "family"):
            raw = self.fixture("assignment")[0]
            del raw["identities"]["program_event_times"][0][key]
            with self.subTest(missing=key), self.assertRaises(driver.Invalid):
                self.normalize(raw)

    def test_target_sender_kind_key_timestamp_sequence_and_fixture_tampering(self):
        for key, wrong in (("sender", "fixture-agent"), ("sender", "user"), ("kind", "transition"), ("kind", []),
                           ("key", "assign:some-other-item"), ("key", "assign:"), ("seq", True), ("text", []),
                           ("at", "not-rfc3339"), ("at", "2026-10-08T12:00:00Z")):
            raw, e, name, target = self.fixture("assignment")
            target[key] = wrong
            if key == "at":
                raw["identities"]["program_event_times"][0]["at"] = wrong
            with self.subTest(key=key, value=wrong), self.assertRaises(driver.Invalid):
                self.normalize(raw)
        for marker in ("relation", "input"):
            raw, e, name, target = self.fixture("assignment")
            if marker == "relation":
                e["relations"][f"fixture_event:{name}#{target['seq']}"] = "injected; not program"
            else:
                e["inputs"].append(f"fixture-event:{name}:" + json.dumps(target))
            with self.subTest(marker=marker), self.assertRaisesRegex(driver.Invalid, "fixture event"):
                self.normalize(raw)
        raw, e, name, target = self.fixture("assignment")
        e["events"][name].append(copy.deepcopy(target))
        with self.assertRaises(driver.Invalid):
            self.normalize(raw)

    def test_tick_call_provenance_is_required_and_bounded(self):
        mutations = [lambda e, n, t: e["relations"].pop(f"event_tick:{n}#{t['seq']}"),
                     lambda e, n, t: e["relations"].update({f"event_tick:{n}#{t['seq']}": "0"}),
                     lambda e, n, t: e["relations"].update({"tick_calls:1": '{"start":0,"end":999}'}),
                     lambda e, n, t: e["relations"].update({"tick_calls:1": '{"start":true,"end":1}'}),
                     lambda e, n, t: e["relations"].update({"tick_calls:1": '{"start":2,"end":1}'})]
        for mutation in mutations:
            raw, e, name, target = self.fixture("assignment")
            mutation(e, name, target)
            with self.subTest(mutation=mutation), self.assertRaises(driver.Invalid):
                self.normalize(raw)

    def test_plan_hash_file_entity_ref_and_mirror_count_tampering(self):
        mutations = [lambda e, n, t: e["relations"].pop("plan_content:e1#1"),
                     lambda e, n, t: e["relations"].update({"plan_content:e1#1": "tampered"}),
                     lambda e, n, t: e["events"][n][0].update(sha256="0" * 64),
                     lambda e, n, t: e["events"][n][0].update(kind="message"),
                     lambda e, n, t: e["events"][n][0].update(file="/other/epic.v1.yaml"),
                     lambda e, n, t: e["events"][n][0].update(file="/other/plan.md"),
                     lambda e, n, t: t.update(key="mirror:99"), lambda e, n, t: t.update(text="mirror 2 blocked-by links of e1 to GitHub"),
                     lambda e, n, t: e["events"][n][2].update(to="checking")]
        for mutation in mutations:
            raw, e, name, target = self.fixture("mirror")
            mutation(e, name, target)
            with self.subTest(mutation=mutation), self.assertRaises(driver.Invalid):
                self.normalize(raw)

    def test_assignment_needs_actual_same_tick_correct_repo_issue_me_and_action(self):
        for read_index, key, wrong in ((1, "tick", 2), (1, "error", "failed user read"),
                                      (2, "repo", "other/repo"), (2, "pr", "13"), (2, "call_index", 1),
                                      (2, "tick", 2), (2, "sha", "wrong-head"), (2, "error", "failed issue read")):
            raw, e, name, target = self.fixture("assignment")
            read = json.loads(e["relations"][f"read_call:{read_index}"])
            read[key] = wrong
            e["relations"][f"read_call:{read_index}"] = json.dumps(read)
            with self.subTest(index=read_index, key=key, wrong=wrong), self.assertRaises(driver.Invalid):
                self.normalize(raw)
        for key, wrong in (("number", True), ("number", 13), ("repository_url", "https://api.github.com/repos/other/repo"),
                           ("assignees", {}), ("assignees", ["not a user"]), ("assignees", [{"login": "actual-user"}])):
            raw, e, name, target = self.fixture("assignment")
            read = json.loads(e["relations"]["read_call:2"])
            response = json.loads(read["stdout"])
            response[key] = wrong
            read["stdout"] = json.dumps(response)
            e["relations"]["read_call:2"] = json.dumps(read)
            with self.subTest(response_key=key, wrong=wrong), self.assertRaises(driver.Invalid):
                self.normalize(raw)
        raw, e, name, target = self.fixture("assignment")
        e["calls"][3]["args"][2] = "13"
        with self.assertRaises(driver.Invalid):
            self.normalize(raw)
        raw, e, name, target = self.fixture("assignment", already=True)
        target["text"] = "assigned"
        with self.assertRaises(driver.Invalid):
            self.normalize(raw)

    def test_handover_and_cancel_reference_reason_owner_and_request_tampering(self):
        cases = [("handover", lambda e, n, t: e["events"][n][1].update(to="implementing")),
                 ("handover", lambda e, n, t: e["events"][n][2].update(file="/other/pr.md")),
                 ("handover", lambda e, n, t: t.update(text="arbitrary handover reason")),
                 ("cancel_done", lambda e, n, t: e["events"][n][1].update(trigger_ref="99")),
                 ("cancel_done", lambda e, n, t: e["events"][n][0].update(request="retry")),
                 ("cancel_done", lambda e, n, t: t.update(text="cancelled: other reason")),
                 ("cancel_cleanup", lambda e, n, t: t.update(recipient="planner")),
                 ("cancel_cleanup", lambda e, n, t: t.update(request="worktree")),
                 ("cancel_cleanup", lambda e, n, t: e["events"][n][0].update(request="not-worktree")),
                 ("cancel_unassign", lambda e, n, t: t.update(text="unassign other/repo#12")),
                 ("cancel_unassign", lambda e, n, t: e["events"][n][1].update(text="already assigned")),
                 ("cancel_unassign", lambda e, n, t: e["calls"][-1]["args"].__setitem__(2, "13")),
                 ("stop_propagation", lambda e, n, t: e["relations"].update({"inputs:e1-s1": "id: e1-s1\nitems: [other]\n"})),
                 ("stop_propagation", lambda e, n, t: t.update(text="wrong cancellation reason")),
                 ("stop_refused", lambda e, n, t: t.update(trigger_ref="1")),
                 ("stop_refused", lambda e, n, t: t.update(trigger="timeout")),
                 ("stop_refused", lambda e, n, t: t.update(**{"from": "queued"})),
                 ("cancel_message", lambda e, n, t: e["relations"].update({"inputs:e1-w1": "id: e1-w1\nset: other\n"})),
                 ("cancel_message", lambda e, n, t: e["relations"].update({"inputs:e1-s1-orchestrator": "task: e1-s1\ncomponent: planner\n"})),
                 ("cancel_message", lambda e, n, t: t.update(text=t["text"].replace("reason", "wrong reason")))]
        for family, mutation in cases:
            raw, e, name, target = self.fixture(family)
            mutation(e, name, target)
            with self.subTest(family=family, mutation=mutation), self.assertRaises(driver.Invalid):
                self.normalize(raw)

    def test_worker_and_program_mappings_coexist_but_never_overlap(self):
        raw, e, name, target = self.fixture("cancel_cleanup")
        worker = {"seq": target["seq"] + 1, "at": "2026-10-19T03:08:00Z", "kind": "done", "sender": "factory",
                  "key": "repo:" + str(target["seq"]), "trigger_ref": str(target["seq"]), "text": "removed"}
        e["events"][name].append(worker)
        raw["identities"]["worker_event_times"] = [{"scenario": "checkpoint", "step": "open_step", "entity": name,
                                                   "seq": worker["seq"], "at": worker["at"]}]
        self.normalize(raw)
        raw["identities"]["program_event_times"].append({**raw["identities"]["worker_event_times"][0], "family": "cancel_done"})
        with self.assertRaisesRegex(driver.Invalid, "overlapping worker/program"):
            self.normalize(raw)

    def test_no_global_timestamp_replacement_status_erasure_or_token_blessing(self):
        raw, e, name, target = self.fixture("assignment")
        e["posts"].append(target["at"])
        target["other_at"] = target["at"]
        normalized = self.normalize(raw)
        step = next(s for s in normalized["scenarios"] if s["name"] == "checkpoint")["steps"][0]["evidence"]
        self.assertEqual(step["events"][name][1]["at"], "<PROGRAM_AT:e1-w1:2>")
        self.assertEqual(step["events"][name][1]["other_at"], target["at"])
        self.assertEqual(step["posts"][-1], target["at"])
        self.assertEqual(step["states"][name]["observed_at"], target["at"])
        for key, value in (("state", "merged"), ("lease", 99), ("head", "other-head"), ("observed_at", "different-time"), ("duration", 123)):
            changed = copy.deepcopy(raw)
            changed_e = next(s for s in changed["trace"]["scenarios"] if s["name"] == "checkpoint")["steps"][0]["evidence"]
            changed_e["states"][name][key] = value
            with self.subTest(status=key):
                self.assertIsNotNone(driver.first_difference(normalized, self.normalize(changed)))
        changed = copy.deepcopy(normalized)
        next(s for s in changed["scenarios"] if s["name"] == "checkpoint")["steps"][0]["evidence"]["events"][name][1]["at"] = "<PROGRAM_AT:other:2>"
        self.assertIsNotNone(driver.first_difference(normalized, changed))
        target["at"] = "<PROGRAM_AT:e1-w1:2>"
        raw["identities"]["program_event_times"][0]["at"] = target["at"]
        with self.assertRaisesRegex(driver.Invalid, "already contains normalization tokens"):
            self.normalize(raw)


class ProvenanceTests(unittest.TestCase):
    def test_every_git_operation_disables_replacements_and_ambient_git_overrides(self):
        operations = [("fetch", "origin", "main"), ("rev-parse", "HEAD^{commit}"),
                      ("merge-base", "main", "candidate"), ("archive", "main"),
                      ("ls-tree", "-r", "main"), ("cat-file", "blob", "a" * 40)]
        with patch.dict(os.environ, {"GIT_NO_REPLACE_OBJECTS": "0", "GIT_REPLACE_REF_BASE": "refs/evil/",
                                     "GIT_DIR": "/foreign", "GIT_CONFIG_COUNT": "1", "GH_TOKEN": "secret"}), \
             patch.object(driver, "bounded", return_value=b"") as run:
            for operation in operations:
                driver.git(Path("."), *operation)
            self.assertEqual(run.call_count, len(operations))
            for call in run.call_args_list:
                argv, cwd, env, timeout = call.args
                self.assertIn("--no-replace-objects", argv)
                self.assertEqual(env["GIT_NO_REPLACE_OBJECTS"], "1")
                for forbidden in ("GIT_REPLACE_REF_BASE", "GIT_DIR", "GIT_CONFIG_COUNT", "GH_TOKEN"):
                    self.assertNotIn(forbidden, env)

    def test_replacement_ref_cannot_substitute_committed_revision(self):
        # Future offline fixture only: Git plumbing writes local objects/replace
        # refs in an isolated bare repo. No checkout, remote or live repo changes.
        with tempfile.TemporaryDirectory() as temp:
            repo = Path(temp)
            env = {"PATH": "/usr/bin:/bin", "HOME": str(repo), "GIT_CONFIG_NOSYSTEM": "1",
                   "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0", "GIT_EDITOR": "true",
                   "GIT_AUTHOR_NAME": "fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
                   "GIT_COMMITTER_NAME": "fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
                   "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+0000", "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+0000"}

            def local(*args, data=None):
                return subprocess.run(["/usr/bin/git", "--no-pager", "--no-optional-locks", *args],
                                      cwd=repo, env=env, input=data, capture_output=True,
                                      check=True, timeout=10).stdout

            def revision(body):
                blob = local("hash-object", "-w", "--stdin", data=body).strip()
                tree = local("mktree", data=b"100644 blob " + blob + b"\tsource.go\n").strip().decode()
                return local("commit-tree", tree, data=b"offline fixture\n").strip().decode()

            local("init", "--bare")
            original = revision(b"package original\n")
            replacement = revision(b"package replacement\n")
            local("update-ref", "refs/replace/" + original, replacement)
            self.assertEqual(local("show", original + ":source.go"), b"package replacement\n")
            self.assertEqual(driver.resolve(repo, original), original)
            files, records = driver.committed_tree(repo, original)
            self.assertEqual(files["source.go"][0], b"package original\n")
            self.assertEqual(records["source.go"]["sha256"], hashlib.sha256(b"package original\n").hexdigest())

    def tracked_harness_repo(self, root):
        repo = root / "repo"
        repo.mkdir()
        env = {"PATH": "/usr/bin:/bin", "HOME": str(root), "GIT_CONFIG_NOSYSTEM": "1",
               "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0", "GIT_EDITOR": "true",
               "GIT_NO_REPLACE_OBJECTS": "1", "GIT_AUTHOR_NAME": "fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
               "GIT_COMMITTER_NAME": "fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
               "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+0000", "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+0000"}

        def local(*args):
            return subprocess.run(["/usr/bin/git", "--no-pager", "--no-optional-locks", "--no-replace-objects",
                                   "-c", "core.hooksPath=/dev/null", *args], cwd=repo, env=env,
                                  capture_output=True, check=True, timeout=10).stdout.decode().strip()

        local("init")
        production = repo / "internal/reconcile/testdata/existing.yaml"
        production.parent.mkdir(parents=True)
        production.write_bytes(b"original: production fixture\n")
        production.chmod(0o644)
        local("add", "--all")
        local("-c", "commit.gpgsign=false", "commit", "--no-gpg-sign", "-m", "isolated production fixture")
        baseline = local("rev-parse", "HEAD")
        for name in driver.SHARED + driver.CANDIDATE:
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"package fixture\n")
            path.chmod(0o644)
        local("add", "--all")
        local("-c", "commit.gpgsign=false", "commit", "--no-gpg-sign", "-m", "isolated matching harness fixture")
        return repo, baseline, local("rev-parse", "HEAD"), local

    def test_real_committed_matching_harness_reused_without_rewriting(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repo, baseline, candidate, local = self.tracked_harness_repo(root)
            baseline_files, baseline_manifest = driver.committed_tree(repo, baseline)
            candidate_files, candidate_manifest = driver.committed_tree(repo, candidate)
            driver.check_dirty(repo)
            overlay = driver.read_overlay(repo, [baseline_manifest, candidate_manifest])
            inserted = []
            original_open = Path.open

            def record_open(path, mode="r", *args, **kwargs):
                if mode == "xb":
                    inserted.append(str(path))
                return original_open(path, mode, *args, **kwargs)

            with patch.object(Path, "open", new=record_open):
                staged = driver.stage(root / "candidate", candidate_files, candidate_manifest, overlay)
            self.assertEqual(inserted, [])
            self.assertEqual(staged, {k: {"sha256": v["sha256"], "mode": v["mode"]} for k, v in candidate_manifest.items()})
            shared = {name: overlay[name] for name in driver.SHARED}
            main_staged = driver.stage(root / "baseline", baseline_files, baseline_manifest, shared)
            self.assertTrue(all(name in main_staged for name in driver.SHARED))
            self.assertTrue(all(name not in main_staged for name in driver.CANDIDATE))
            # After merge, a baseline can itself contain the exact tracked tests.
            driver.read_overlay(repo, [candidate_manifest, candidate_manifest])
            merged = driver.stage(root / "postmerge", candidate_files, candidate_manifest, shared)
            self.assertEqual(merged, staged)
            self.assertTrue(all((root / "postmerge" / name).is_file() for name in driver.CANDIDATE))

    def test_real_committed_older_harness_replaced_but_original_production_and_bytes_preserved(self):
        self.assertEqual(driver.REPLACEABLE_HARNESS, {
            "internal/reconcile/claude_compat_test.go", "cmd/factory/claude_compat_test.go",
            "internal/reconcile/claude_compat_candidate_test.go", "cmd/factory/claude_compat_candidate_test.go"})
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repo, baseline, candidate, local = self.tracked_harness_repo(root)
            baseline_files, baseline_manifest = driver.committed_tree(repo, baseline)
            candidate_files, candidate_manifest = driver.committed_tree(repo, candidate)
            original_manifest = copy.deepcopy(candidate_manifest)
            for name in driver.SHARED + driver.CANDIDATE:
                (repo / name).write_bytes(b"package current_fixture\n// " + name.encode() + b"\n")
            driver.check_dirty(repo)  # Exact owned dirty tests only.
            overlay = driver.read_overlay(repo, [baseline_manifest, candidate_manifest])
            cases = (("pre-harness-main", "main-default", baseline_files, baseline_manifest),
                     ("postmerge-main", "main-default", candidate_files, candidate_manifest),
                     ("candidate", "candidate-default", candidate_files, candidate_manifest),
                     ("explicit", "candidate-explicit", candidate_files, candidate_manifest))
            for label, lane, files, manifest in cases:
                selected = driver.overlay_for_lane(lane, manifest, overlay)
                staged = driver.stage(root / label, files, manifest, selected)
                actions = driver.overlay_disposition(manifest, selected)
                for name, data in selected.items():
                    self.assertEqual((root / label / name).read_bytes(), data)
                    self.assertEqual(staged[name]["sha256"], hashlib.sha256(data).hexdigest())
                    self.assertEqual(actions[name]["new_sha256"], hashlib.sha256(data).hexdigest())
                    if name in manifest:
                        self.assertEqual(actions[name]["disposition"], "replaced_test_harness")
                        self.assertEqual(actions[name]["prior_sha256"], hashlib.sha256(b"package fixture\n").hexdigest())
                        self.assertEqual(base64.b64decode(manifest[name]["content_base64"]), b"package fixture\n")
                        self.assertEqual(manifest[name]["mode"], 0o644)
                        self.assertEqual(actions[name]["prior_git_blob"], manifest[name]["git_blob"])
                    else:
                        self.assertEqual(actions[name]["disposition"], "inserted")
                        self.assertIsNone(actions[name]["prior_sha256"])
                        self.assertIsNone(actions[name]["prior_mode"])
                        self.assertIsNone(actions[name]["prior_git_blob"])
                for name in set(manifest) - set(selected):
                    self.assertEqual(staged[name]["sha256"], manifest[name]["sha256"])
                    self.assertEqual((root / label / name).read_bytes(), files[name][0])
            self.assertEqual(candidate_manifest, original_manifest)
            self.assertEqual(candidate_files[driver.SHARED[0]][0], b"package fixture\n")
            self.assertTrue(all(not (root / "pre-harness-main" / p).exists() for p in driver.CANDIDATE))
            self.assertTrue(all((root / "postmerge-main" / p).read_bytes() == overlay[p] for p in driver.CANDIDATE))
            for name in driver.SHARED:
                self.assertEqual(len({(root / label / name).read_bytes() for label, *_ in cases}), 1)
            # Hash metadata is not allowed to lie about the original committed blob.
            forged = copy.deepcopy(candidate_manifest)
            forged[driver.SHARED[0]]["sha256"] = hashlib.sha256(overlay[driver.SHARED[0]]).hexdigest()
            with self.assertRaisesRegex(driver.Invalid, "committed overlay source provenance mismatch"):
                driver.read_overlay(repo, [forged])

    def test_only_current_overlay_requires_0644_and_old_test_mode_is_audited(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repo, baseline, candidate, local = self.tracked_harness_repo(root)
            files, manifest = driver.committed_tree(repo, candidate)
            path = repo / driver.SHARED[0]
            path.chmod(0o755)
            with self.assertRaisesRegex(driver.Invalid, "regular mode 0644"):
                driver.read_overlay(repo, [manifest])
            local("add", "--chmod=+x", "--", driver.SHARED[0])
            local("-c", "commit.gpgsign=false", "commit", "--no-gpg-sign", "-m", "isolated prior-mode fixture")
            prior_files, prior_manifest = driver.committed_tree(repo, local("rev-parse", "HEAD"))
            path.chmod(0o644)
            overlay = driver.read_overlay(repo, [prior_manifest])
            staged = driver.stage(root / "new-mode", prior_files, prior_manifest, overlay)
            action = driver.overlay_disposition(prior_manifest, overlay)[driver.SHARED[0]]
            self.assertEqual(action["disposition"], "replaced_test_harness")
            self.assertEqual(action["prior_mode"], 0o755)
            self.assertEqual(action["new_mode"], 0o644)
            self.assertEqual(action["prior_sha256"], action["new_sha256"])
            self.assertEqual(prior_manifest[driver.SHARED[0]]["mode"], 0o755)
            self.assertEqual(staged[driver.SHARED[0]]["mode"], 0o644)

    def test_production_fixture_collision_rejected_even_if_identical(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repo, baseline, candidate, local = self.tracked_harness_repo(root)
            files, manifest = driver.committed_tree(repo, candidate)
            production = "internal/reconcile/testdata/existing.yaml"
            with self.assertRaisesRegex(driver.Invalid, "unagreed overlay path"):
                driver.stage(root / "production", files, manifest, {production: files[production][0]})
            with patch.object(driver, "FIXTURES", (production,)):
                with self.assertRaisesRegex(driver.Invalid, "exact agreed claude_compat subtree"):
                    driver.read_overlay(repo, [manifest])
            self.assertEqual((repo / production).read_bytes(), b"original: production fixture\n")

    def test_exact_agreed_tracked_fixture_may_be_reused_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            repo, baseline, candidate, local = self.tracked_harness_repo(root)
            name = "internal/reconcile/testdata/claude_compat/agreed.txt"
            path = repo / name
            path.parent.mkdir(parents=True)
            path.write_bytes(b"agreed fixture\n")
            path.chmod(0o644)
            local("add", "--", name)
            local("-c", "commit.gpgsign=false", "commit", "--no-gpg-sign", "-m", "isolated agreed fixture")
            files, manifest = driver.committed_tree(repo, local("rev-parse", "HEAD"))
            with patch.object(driver, "FIXTURES", (name,)):
                driver.check_dirty(repo)
                overlay = driver.read_overlay(repo, [manifest])
                driver.stage(root / "staged", files, manifest, overlay)
                self.assertEqual((root / "staged" / name).read_bytes(), b"agreed fixture\n")
                path.write_bytes(b"not the committed fixture\n")
                with self.assertRaises(driver.Invalid):
                    driver.read_overlay(repo, [manifest])
                path.write_bytes(b"agreed fixture\n")
                path.chmod(0o755)
                local("add", "--chmod=+x", "--", name)
                local("-c", "commit.gpgsign=false", "commit", "--no-gpg-sign", "-m", "isolated wrong fixture mode")
                old_files, old_manifest = driver.committed_tree(repo, local("rev-parse", "HEAD"))
                path.chmod(0o644)
                with self.assertRaisesRegex(driver.Invalid, "tracked fixture differs"):
                    driver.read_overlay(repo, [old_manifest])
                with self.assertRaisesRegex(driver.Invalid, "tracked fixture differs"):
                    driver.stage(root / "forbidden-mode-rewrite", old_files, old_manifest, overlay)

    def test_overlay_collisions_missing_files_and_symlinks(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name in driver.SHARED + driver.CANDIDATE:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("package fixture")
            self.assertEqual(set(driver.read_overlay(root, [{}, {}])), set(driver.SHARED + driver.CANDIDATE))
            with self.assertRaisesRegex(driver.Invalid, "committed overlay source provenance mismatch"):
                driver.read_overlay(root, [{driver.SHARED[0]: {}}, {}])
            p = root / driver.SHARED[0]
            p.unlink()
            with self.assertRaisesRegex(driver.Invalid, "missing"):
                driver.read_overlay(root, [{}, {}])
            p.symlink_to(root / driver.SHARED[1])
            with self.assertRaisesRegex(driver.Invalid, "regular"):
                driver.read_overlay(root, [{}, {}])

    def test_dirty_production_and_unagreed_overlays_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            for status in (b" M internal/reconcile/tick.go\0", b"?? internal/reconcile/extra_test.go\0",
                           b"R  new.go\0old.go\0"):
                with patch.object(driver, "git", side_effect=[b"", status]), self.assertRaises(driver.Invalid):
                    driver.check_dirty(Path(temp))
            status = b"?? scripts/claude-compat.py\0?? internal/reconcile/claude_compat_test.go\0"
            with patch.object(driver, "git", side_effect=[b"", status]):
                driver.check_dirty(Path(temp))

    def test_hidden_dirty_index_states_rejected(self):
        for flag in (b"h", b"S"):
            with patch.object(driver, "git", return_value=flag + b" internal/reconcile/tick.go\0"):
                with self.assertRaisesRegex(driver.Invalid, "index state"):
                    driver.check_dirty(Path("."))

    def test_untouched_source_and_overlay_hashes_are_independent(self):
        with tempfile.TemporaryDirectory() as temp:
            source = {"source.go": (b"package original", 0o644)}
            manifest = {"source.go": {"sha256": driver.digest(b"package original"), "mode": 0o644, "git_blob": "abc"}}
            root = Path(temp) / "source"
            staged = driver.stage(root, source, manifest, {driver.SHARED[0]: b"package test"})
            self.assertEqual(staged["source.go"]["sha256"], manifest["source.go"]["sha256"])
            self.assertEqual(staged[driver.SHARED[0]]["sha256"], driver.digest(b"package test"))
            (root / "source.go").write_bytes(b"package changed")
            self.assertNotEqual(driver.tree_manifest(root), staged)
            with self.assertRaisesRegex(driver.Invalid, "untouched-source"):
                driver.stage(Path(temp) / "bad", {"source.go": (b"package candidate", 0o644)}, manifest, {})

    def test_unsafe_archive_paths_and_links_rejected(self):
        for name in ("../escape", "/absolute", ".git/config", "a/../b"):
            with self.subTest(name=name), self.assertRaises(driver.Invalid):
                driver.safe_path(name)
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as archive:
            info = tarfile.TarInfo("linked")
            info.type = tarfile.SYMTYPE
            info.linkname = "/outside"
            archive.addfile(info)
        with patch.object(driver, "git", side_effect=[b"120000 blob abc\tlinked\0", stream.getvalue()]), self.assertRaisesRegex(driver.Invalid, "unsupported Git entry"):
            driver.committed_tree(Path("."), "a" * 40)

    def test_archive_bytes_must_match_independent_git_blob(self):
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as archive:
            data = b"candidate pretending to be main"
            info = tarfile.TarInfo("source.go")
            info.size, info.mode = len(data), 0o644
            archive.addfile(info, io.BytesIO(data))
        with patch.object(driver, "git", side_effect=[b"100644 blob abc\tsource.go\0", stream.getvalue(), b"actual main"]) as query:
            with self.assertRaisesRegex(driver.Invalid, "changed committed file"):
                driver.committed_tree(Path("."), "a" * 40)
            self.assertEqual(query.call_args_list[1].args[1:5], ("-c", "tar.umask=0022", "archive", "--format=tar"))

    def test_environment_does_not_inherit_credentials_or_go_flags(self):
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {"GH_TOKEN": "secret", "SSH_AUTH_SOCK": "/live/socket", "GOFLAGS": "-exec=bad"}):
            env = driver.private_env(Path(temp), "/local/go", Path(temp) / "cache", "/local/modules")
            self.assertNotIn("GH_TOKEN", env)
            self.assertNotIn("SSH_AUTH_SOCK", env)
            self.assertEqual(env["GOFLAGS"], "-mod=readonly")
            self.assertEqual(env["GOTOOLCHAIN"], "local")
            self.assertEqual(env["GOPROXY"], "off")
            self.assertEqual(env["GIT_NO_REPLACE_OBJECTS"], "1")
            self.assertNotEqual(env["HOME"], env["TMPDIR"])
            self.assertNotEqual(env["FACTORY_BRAIN"], env["HOME"])

    def test_default_mode_only_compiles_never_executes_binaries(self):
        with tempfile.TemporaryDirectory(dir=Path("/tmp").resolve(), prefix="fc-test-") as temp:
            root = Path(temp)
            scratch = root / "retained"
            scratch.mkdir(mode=0o700)
            repo = root / "repo"
            (repo / "scripts").mkdir(parents=True)
            script = repo / "scripts/claude-compat.py"
            script.write_text("# driver identity")
            for name in driver.SHARED + driver.CANDIDATE:
                path = repo / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("package fixture")
            modules = root / "modules"
            modules.mkdir()
            go = root / "go"
            go.write_text("fake toolchain identity")
            sha = "a" * 40
            prior_harness = b"package previous_fixture"
            files = {"go.mod": (b"module offline", 0o644), driver.SHARED[0]: (prior_harness, 0o644),
                     driver.CANDIDATE[0]: (b"package fixture", 0o644)}
            records = {"go.mod": {"sha256": driver.digest(b"module offline"), "mode": 0o644, "git_blob": "blob"},
                       driver.SHARED[0]: {"sha256": driver.digest(prior_harness), "mode": 0o644, "git_blob": "b" * 40,
                                          "content_base64": base64.b64encode(prior_harness).decode()},
                       driver.CANDIDATE[0]: {"sha256": driver.digest(b"package fixture"), "mode": 0o644, "git_blob": "c" * 40,
                                             "content_base64": "cGFja2FnZSBmaXh0dXJl"}}
            commands = []

            def fake_git(repo, *args):
                if args[0] == "cat-file":
                    return prior_harness if args[2] == "b" * 40 else b"package fixture"
                if args[0] == "worktree":
                    return b"worktree " + str(repo).encode() + b"\0\0"
                if args[0] in {"status", "ls-files"}:
                    return b""
                if args[0] == "merge-base":
                    return sha.encode()
                if args[0] == "--version":
                    return b"git offline"
                self.fail(f"unexpected git command {args}")

            def fake_bounded(args, cwd, env, timeout, log=None):
                commands.append(args)
                if args[1:] == ["version"]:
                    return b"go local"
                self.assertEqual(args[1:3], ["test", "-c"])
                self.assertNotIn("-test.run", " ".join(args))
                Path(args[4]).write_bytes(b"compiled but never executed")
                return b""

            with patch.object(driver, "__file__", str(script)), patch.object(driver, "resolve", return_value=sha), \
                 patch.object(driver, "committed_tree", return_value=(files, records)), \
                 patch.object(driver, "git", side_effect=fake_git), \
                 patch.object(driver, "bounded", side_effect=fake_bounded), \
                 patch.object(driver.shutil, "which", return_value=str(go)), \
                 patch.object(driver.tempfile, "mkdtemp", return_value=str(scratch)), \
                 patch.object(driver.shutil, "rmtree") as delete, \
                 patch.dict(os.environ, {"GOMODCACHE": str(modules)}):
                self.assertEqual(driver.main(["--output", str(root / "result")]), 0)
                with self.assertRaisesRegex(driver.Invalid, "NEW absolute"):
                    driver.main(["--output", str(root / "result")])
                delete.assert_not_called()
            result = json.loads((root / "result/result.json").read_text())
            self.assertFalse(result["compatibility_pass"])
            self.assertEqual(result["schema_version"], 2)
            self.assertEqual(json.loads((root / "result/provenance.json").read_text())["schema_version"], 2)
            self.assertEqual(result["status"], "compiled-only-NOT-EXECUTED")
            self.assertEqual(result["runtime_status"], "retained")
            self.assertTrue(Path(result["runtime_root"]["path"]).is_dir())
            self.assertEqual(len([c for c in commands if c[1:3] == ["test", "-c"]]), 6)
            for lane, record in result["lanes"].items():
                self.assertEqual(record["overlay_disposition"][driver.SHARED[0]], {
                    "disposition": "replaced_test_harness", "prior_sha256": hashlib.sha256(prior_harness).hexdigest(),
                    "new_sha256": hashlib.sha256(b"package fixture").hexdigest(), "prior_mode": 0o644, "new_mode": 0o644,
                    "prior_git_blob": "b" * 40})
                self.assertEqual(record["overlay_disposition"][driver.SHARED[1]]["disposition"], "inserted")
                self.assertEqual(record["overlay_disposition"][driver.CANDIDATE[0]]["disposition"], "reused")
                self.assertEqual(base64.b64decode(result["sources"]["main"][driver.SHARED[0]]["content_base64"]), prior_harness)
                self.assertEqual(result["sources"]["main"][driver.SHARED[0]]["sha256"], hashlib.sha256(prior_harness).hexdigest())
                self.assertEqual(record["staged"][driver.SHARED[0]]["sha256"], hashlib.sha256(b"package fixture").hexdigest())
                self.assertEqual(json.loads((root / "result" / lane / "overlay-disposition.json").read_text()), record["overlay_disposition"])
            self.assertTrue((root / "result/main-default/source" / driver.CANDIDATE[0]).exists())
            self.assertFalse((root / "result/main-default/source" / driver.CANDIDATE[1]).exists())
            self.assertNotIn(driver.CANDIDATE[1], result["lanes"]["main-default"]["overlay_disposition"])
            self.assertTrue((root / "result/candidate-default/source" / driver.CANDIDATE[1]).exists())


if __name__ == "__main__":
    unittest.main()
