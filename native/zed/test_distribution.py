"""Offline packaging tests; never start a native binary or access credentials."""
import hashlib
from pathlib import Path
import shutil
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import build

REQUIRED = {
    'Cargo.lock',
    'README.md',
    'crates/agent/src/agent.rs',
    'crates/agent/src/db.rs',
    'crates/eval_cli/Cargo.toml',
    'crates/eval_cli/README.md',
    'crates/eval_cli/src/credential_probe.rs',
    'crates/eval_cli/src/headless.rs',
    'crates/eval_cli/src/lifecycle_managed_tests.rs',
    'crates/eval_cli/src/lifecycle_probe.rs',
    'crates/eval_cli/src/lifecycle_protocol.rs',
    'crates/eval_cli/src/lifecycle_tests.rs',
    'crates/eval_cli/src/main.rs',
    'crates/eval_cli/src/managed_tool_files.rs',
    'crates/eval_cli/src/managed_tool_project_fs.rs',
    'crates/eval_cli/src/managed_tools.rs',
    'crates/eval_cli/src/newnativeauth_usage.rs',
    'crates/eval_cli/src/usage_probe.rs',
    'crates/language_models/src/provider/openai_subscribed.rs',
    'crates/openai_subscribed/Cargo.toml',
    'crates/openai_subscribed/src/native_auth.rs',
    'crates/openai_subscribed/src/native_auth/tests.rs',
    'crates/openai_subscribed/src/openai_subscribed.rs',
    'crates/openai_subscribed/src/usage.rs',
    'crates/openai_subscribed/src/usage/tests.rs',
}


class DistributionTests(unittest.TestCase):
    def setUp(self):
        (build.ROOT / '.verification').mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=build.ROOT / '.verification')
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)

    def package_copy(self):
        root = self.directory / 'package'
        root.mkdir()
        for name in ('LICENSE', 'source-manifest.json'):
            shutil.copyfile(build.ROOT / name, root / name)
        for name in ('overlay', 'notices'):
            shutil.copytree(build.ROOT / name, root / name)
        return root

    def test_required_source_and_manual_notice(self):
        manifest = build.verify_package()
        self.assertEqual(set(manifest['overlay']), REQUIRED)
        notice = '> [!IMPORTANT]\n> Remove this line to confirm you\'ve reviewed this PR before submitting.\n'
        self.assertTrue((build.ROOT / 'overlay/README.md').read_text().startswith(notice))
        self.assertIn('GPL-3.0-or-later', (build.ROOT / 'overlay/crates/eval_cli/Cargo.toml').read_text())

    def test_overlay_tamper(self):
        root = self.package_copy()
        with (root / 'overlay/Cargo.lock').open('a') as file:
            file.write('\n# tampered\n')
        with self.assertRaisesRegex(ValueError, 'source hash mismatch: overlay/Cargo.lock'):
            build.verify_package(root)

    def test_notice_tamper(self):
        root = self.package_copy()
        (root / 'notices/LICENSE-APACHE').write_text('changed')
        with self.assertRaisesRegex(ValueError, 'source hash mismatch: notices/LICENSE-APACHE'):
            build.verify_package(root)

    def test_manifest_tamper(self):
        root = self.package_copy()
        (root / 'source-manifest.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'source manifest hash mismatch'):
            build.verify_package(root)

    def test_missing_untracked_source(self):
        root = self.package_copy()
        (root / 'overlay/crates/eval_cli/src/lifecycle_probe.rs').unlink()
        with self.assertRaisesRegex(ValueError, 'unexpected or missing'):
            build.verify_package(root)

    def test_extra_private_source_rejected(self):
        root = self.package_copy()
        (root / 'overlay/.env').write_text('private')
        with self.assertRaisesRegex(ValueError, 'unexpected or missing'):
            build.verify_package(root)

    def test_empty_private_directory_rejected(self):
        root = self.package_copy()
        (root / 'overlay/.ssh').mkdir()
        with self.assertRaisesRegex(ValueError, 'unexpected source directory: overlay/.ssh'):
            build.verify_package(root)

    def test_explicit_paths_and_bounded_jobs_required(self):
        for arguments, message in [((), '--checkout and --output are required'),
                                   (('--jobs', '5'), 'invalid choice'),
                                   (('--seconds', '3601'), '--seconds must be between')]:
            result = subprocess.run(['python3', str(build.ROOT / 'build.py'), *arguments],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2)
            self.assertIn(message, result.stderr)

    def test_symlink_rejected(self):
        root = self.package_copy()
        target = root / 'overlay/Cargo.lock'
        target.unlink()
        target.symlink_to(build.ROOT / 'overlay/Cargo.lock')
        with self.assertRaisesRegex(ValueError, 'symlink in packaged source'):
            build.verify_package(root)

    def test_wrong_checkout_pin_without_writes(self):
        with patch.object(build, 'git_read', return_value='0' * 40):
            with self.assertRaisesRegex(ValueError, 'HEAD does not match pinned'):
                build.verify_checkout(self.directory)

    def test_correct_checkout_pin(self):
        with patch.object(build, 'git_read', side_effect=[build.PIN, 'commit']):
            build.verify_checkout(self.directory)

    def test_all_executables_and_managed_sources_included(self):
        manifest = build.verify_package()
        self.assertEqual(set(build.BINARIES), {'eval-cli', 'zed-native-usage-probe',
                         'zed-native-lifecycle-probe', 'zed-native-credential-setup'})
        normalizations = manifest['documentation_normalizations']
        self.assertEqual(set(normalizations), {'crates/eval_cli/README.md'})
        for name, record in manifest['overlay'].items():
            if name not in normalizations:
                self.assertEqual(record['sha256'], record['auxiliary_sha256'], name)
        cargo = (build.ROOT / 'overlay/crates/eval_cli/Cargo.toml').read_text()
        for binary in build.BINARIES:
            self.assertIn('name = "' + binary + '"', cargo)

    def test_overlay_application_and_baseline_tamper(self):
        root = self.directory / 'fixture'
        overlay = root / 'overlay'
        overlay.mkdir(parents=True)
        (overlay / 'old').write_text('after')
        (overlay / 'new').write_text('added')
        source = self.directory / 'source'
        source.mkdir()
        (source / 'old').write_text('wrong')
        manifest = {'overlay': {
            'old': {'base_sha256': hashlib.sha256(b'before').hexdigest(), 'sha256': build.digest(overlay/'old')},
            'new': {'base_sha256': None, 'sha256': build.digest(overlay/'new')},
        }}
        with self.assertRaisesRegex(ValueError, 'baseline hash mismatch: old'):
            build.apply_overlay(source, manifest, root)
        self.assertFalse((source / 'new').exists())
        (source / 'old').write_text('before')
        build.apply_overlay(source, manifest, root)
        self.assertEqual((source / 'old').read_text(), 'after')
        self.assertEqual((source / 'new').read_text(), 'added')

    def test_timeout_preserves_source(self):
        marker = self.directory / 'source.rs'
        marker.write_text('useful source')
        with self.assertRaisesRegex(TimeoutError, 'deadline exceeded'):
            build.Runner(0.1).run(['python3', '-c', 'import time; time.sleep(30)'], self.directory)
        self.assertEqual(marker.read_text(), 'useful source')

    def test_package_has_no_private_paths_credentials_or_binaries(self):
        denied = {'.git', '.env', '.ssh', '.aws', 'target', 'account', 'credentials',
                  'usage', 'sessions', 'node_modules', 'runtime', 'run', 'data'}
        # "usage" is the reviewed Rust source module, not a runtime directory.
        for path in build.ROOT.rglob('*'):
            relative = path.relative_to(build.ROOT)
            if relative.parts[0] in {'.verification', '__pycache__'}:
                continue
            if relative.as_posix() == 'overlay/crates/openai_subscribed/src/usage' or relative.as_posix().startswith('overlay/crates/openai_subscribed/src/usage/'):
                blocked = denied - {'usage'}
            else:
                blocked = denied
            self.assertFalse(set(relative.parts) & blocked, relative)
            self.assertFalse(path.is_symlink(), relative)
            if not path.is_file():
                continue
            data = path.read_bytes()
            self.assertNotIn(b'\x00', data, relative)
            personal_paths = re.findall(rb"/(?:Users|home)/([^/\s\"'<>\x60]+)", data)
            self.assertFalse(set(personal_paths) - {b'user'}, relative)
            self.assertNotIn(b'-----BEGIN ' + b'PRIVATE KEY-----', data, relative)


if __name__ == '__main__':
    (build.ROOT / '.verification').mkdir(exist_ok=True)
    unittest.main()
