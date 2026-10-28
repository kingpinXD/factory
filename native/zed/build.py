#!/usr/bin/env python3
"""Verify and build the pinned native overlay without changing a selected checkout."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

PIN = 'fb69067729cb16a7f6135e2c1600c26a28c59659'
RUST = '1.98.1'
BUILD_FEATURE = 'util/debug-embed'
ASSET_TEST = 'tests::runtime_assets_load_without_native_startup'
MANIFEST_SHA256 = 'd69c43b7ec306c809d18087978fd516e769acf5c0d4b66c225d653c8a4dc554a'
ROOT = Path(__file__).resolve().parent
BINARIES = ('eval-cli', 'zed-native-usage-probe', 'zed-native-lifecycle-probe',
            'zed-native-credential-setup')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_package(root=ROOT):
    manifest_path = root / 'source-manifest.json'
    if digest(manifest_path) != MANIFEST_SHA256:
        raise ValueError('source manifest hash mismatch')
    manifest = json.loads(manifest_path.read_text())
    if manifest['commit'] != PIN or manifest['rust'] != RUST:
        raise ValueError('manifest pin/toolchain mismatch')
    for field, key in [('auxiliary_snapshot_sha256', 'auxiliary_sha256'),
                       ('overlay_snapshot_sha256', 'sha256')]:
        inventory = ''.join(record[key] + '  ' + name + '\n'
                            for name, record in sorted(manifest['overlay'].items()))
        if hashlib.sha256(inventory.encode()).hexdigest() != manifest[field]:
            raise ValueError('snapshot inventory hash mismatch: ' + field)
    if manifest['cargo_features'] != [BUILD_FEATURE]:
        raise ValueError('manifest portable asset feature mismatch')
    expected = {'overlay/' + name: record['sha256']
                for name, record in manifest['overlay'].items()}
    expected.update(manifest['notices'])
    allowed = set(expected) - {'LICENSE'}
    directories = {str(parent) for name in allowed for parent in Path(name).parents}
    paths = [p for directory in ('overlay', 'notices')
             for p in (root / directory).rglob('*')]
    for path in paths:
        name = path.relative_to(root).as_posix()
        if path.is_symlink():
            raise ValueError('symlink in packaged source: ' + name)
        if path.is_dir() and name not in directories:
            raise ValueError('unexpected source directory: ' + name)
    actual = {p.relative_to(root).as_posix() for p in paths if not p.is_dir()}
    if actual != allowed:
        raise ValueError('unexpected or missing overlay/notice source files')
    for name, sha256 in expected.items():
        path = root / name
        if any(parent.is_symlink() for parent in (path, *path.parents)):
            raise ValueError('symlink in packaged source: ' + name)
        if digest(path) != sha256:
            raise ValueError('source hash mismatch: ' + name)
    return manifest


def git_read(checkout, *args):
    return subprocess.check_output(
        ['git', '--no-optional-locks', '-C', str(checkout), *args],
        timeout=30, stderr=subprocess.PIPE).decode().strip()


def verify_checkout(checkout):
    if git_read(checkout, 'rev-parse', 'HEAD') != PIN:
        raise ValueError('selected checkout HEAD does not match pinned Zed commit')
    if git_read(checkout, 'cat-file', '-t', PIN) != 'commit':
        raise ValueError('pinned object is not a commit')


class Runner:
    def __init__(self, seconds):
        self.deadline = time.monotonic() + seconds

    def run(self, command, cwd, env=None, stdout=None):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('total build deadline exceeded')
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=stdout,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=remaining)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise TimeoutError('build interrupted/deadline exceeded; source and logs retained')
        if code:
            raise RuntimeError('command failed (' + str(code) + '): ' + ' '.join(command))


def apply_overlay(source, manifest, root=ROOT):
    # Check every baseline before copying anything, including absence of new files.
    for name, record in manifest['overlay'].items():
        path = source / name
        baseline = record['base_sha256']
        if baseline is None:
            if path.exists() or path.is_symlink():
                raise ValueError('new source path already exists: ' + name)
        elif not path.is_file() or path.is_symlink() or digest(path) != baseline:
            raise ValueError('baseline hash mismatch: ' + name)
    for name in manifest['overlay']:
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(root / 'overlay' / name, path)
    for name, record in manifest['overlay'].items():
        if digest(source / name) != record['sha256']:
            raise ValueError('applied overlay hash mismatch: ' + name)



def cargo_flags(jobs, allow_network):
    flags = ['--locked', '--jobs', str(jobs), '--features', BUILD_FEATURE]
    if not allow_network:
        flags.append('--offline')
    return flags


def check_runtime_assets(cargo, flags, source, output, env, runner):
    with (output / 'asset-check-build.jsonl').open('w') as log:
        runner.run(cargo + ['test', *flags, '-p', 'eval_cli', '--bin',
                           'zed-native-credential-setup', '--no-run',
                           '--message-format=json'], source, env, log)
    artifacts = []
    for line in (output / 'asset-check-build.jsonl').read_text().splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (item.get('reason') == 'compiler-artifact'
                and item.get('target', {}).get('name') == 'zed-native-credential-setup'
                and item.get('profile', {}).get('test') and item.get('executable')):
            artifacts.append(item['executable'])
    if len(artifacts) != 1:
        raise ValueError('expected exactly one offline runtime asset test executable')
    checks = output / 'checks'
    empty = output / 'empty-cwd'
    home = output / 'empty-home'
    for directory in (checks, empty, home):
        directory.mkdir(mode=0o700)
    for location in (checks, empty):
        if any((parent / 'assets/settings/default.json').exists()
               for parent in (location, *location.parents)):
            raise ValueError('relocated asset check must not have a Zed assets ancestor')
    binary = checks / 'zed-native-runtime-assets-test'
    shutil.copy2(artifacts[0], binary)
    with (output / 'asset-check.log').open('w') as log:
        runner.run([str(binary), '--exact', ASSET_TEST, '--test-threads=1'], empty,
                   {'HOME': str(home), 'PATH': os.environ.get('PATH', '')}, log)
    print('relocated settings/keymap/template asset-only check passed:', binary, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify-only', action='store_true')
    parser.add_argument('--checkout', type=Path, help='existing local checkout at exact pin; read only')
    parser.add_argument('--output', type=Path, help='new experiment directory; never an install path')
    parser.add_argument('--cache-target', type=Path, help='explicit reusable Cargo target (will be written)')
    parser.add_argument('--prepare-only', action='store_true')
    parser.add_argument('--test', action='store_true', help='also run offline eval_cli and subscription tests')
    parser.add_argument('--jobs', type=int, default=2, choices=range(1, 5))
    parser.add_argument('--seconds', type=int, default=600, metavar='1..3600')
    parser.add_argument('--allow-network', action='store_true', help='allow Cargo dependency downloads')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600:
        parser.error('--seconds must be between 1 and 3600')
    manifest = verify_package()
    if args.verify_only:
        print('source manifest, overlay, lockfile and notices verified')
        return
    if args.checkout is None or args.output is None:
        parser.error('--checkout and --output are required (no install defaults)')
    checkout = args.checkout.resolve(strict=True)
    verify_checkout(checkout)
    output = args.output.resolve()
    if output.exists() or output.is_symlink():
        raise ValueError('output must be a new directory; existing paths are never overwritten')
    if not output.parent.is_dir():
        raise ValueError('output parent must already exist')
    if output.is_relative_to(checkout):
        raise ValueError('output must not be inside selected checkout')
    if args.cache_target:
        target = args.cache_target.resolve(strict=True)
        if not target.is_dir():
            raise ValueError('cache target must be an existing directory')
    else:
        target = output / 'target'
    output.mkdir(mode=0o700)
    runner = Runner(args.seconds)
    source = output / 'source'
    print('Owned temporary worktree, logs and any build artifacts retained at:', output, flush=True)
    print('Cargo jobs:', args.jobs, 'total seconds:', args.seconds, 'target:', target, flush=True)
    with (output / 'build.log').open('w') as log:
        # All worktree metadata belongs to this clone, not to the selected checkout.
        runner.run(['git', 'clone', '--bare', '--shared', '--no-hardlinks',
                    str(checkout), str(output / 'repository.git')], output, stdout=log)
        runner.run(['git', '--git-dir=' + str(output / 'repository.git'),
                    'worktree', 'add', '--detach', str(source), PIN], output, stdout=log)
        apply_overlay(source, manifest)
        if args.prepare_only:
            print('prepared source verified; no Rust command executed')
            return
        env = os.environ.copy()
        env.update(CARGO_BUILD_JOBS=str(args.jobs), CARGO_TARGET_DIR=str(target),
                   CARGO_NET_GIT_FETCH_WITH_CLI='true')
        runner.run(['rustup', 'run', RUST, 'rustc', '--version'], source, env, log)
        cargo = ['rustup', 'run', RUST, 'cargo']
        flags = cargo_flags(args.jobs, args.allow_network)
        runner.run(cargo + ['build', *flags, '-p', 'eval_cli', '--bins'], source, env, log)
        if args.test:
            runner.run(cargo + ['test', *flags, '-p', 'eval_cli', '--bins', '--',
                                '--test-threads=' + str(args.jobs)], source, env, log)
            runner.run(cargo + ['test', *flags, '-p', 'openai_subscribed', '--lib', '--',
                                '--test-threads=' + str(args.jobs)], source, env, log)
        check_runtime_assets(cargo, flags, source, output, env, runner)
        (output / 'bin').mkdir()
        for name in BINARIES:
            shutil.copy2(target / 'debug' / name, output / 'bin' / name)
        print('built native experiment executables; only offline asset regression executed')


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except (ValueError, RuntimeError, TimeoutError, OSError, subprocess.SubprocessError) as error:
        print('error:', error, file=sys.stderr)
        sys.exit(1)
