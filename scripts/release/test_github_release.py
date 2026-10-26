"""Exercise GitHub release publication without contacting GitHub or Maven."""

import json
import os
from pathlib import Path
import subprocess
from subprocess import CompletedProcess
import sys
import tempfile
import unittest
from unittest.mock import patch

import github_release


class CommandTests(unittest.TestCase):
    def test_checked_failure_keeps_diagnostic_in_parent_stderr(self):
        child = ('import sys; sys.stderr.write("permission " + '
                 '"denied: missing contents write" + chr(10)); sys.exit(7)')
        script = ('import sys; import github_release; '
                  f'github_release.command(sys.executable, "-c", {child!r})')
        result = subprocess.run([sys.executable, '-c', script], text=True,
                                capture_output=True, timeout=10,
                                env={**os.environ, 'PYTHONPATH': str(Path(__file__).resolve().parent)})
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('permission denied: missing contents write\n', result.stderr)
        self.assertIn('CalledProcessError', result.stderr)

    def test_unchecked_probe_retains_captured_output(self):
        result = github_release.command(sys.executable, '-c',
                                        'import sys; print("probe"); '
                                        'sys.stderr.write("not found"); sys.exit(1)', check=False)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, 'probe\n')
        self.assertEqual(result.stderr, 'not found')


class GitHubReleaseTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.previous = Path.cwd()
        os.chdir(self.directory.name)
        self.addCleanup(os.chdir, self.previous)
        self.addCleanup(self.directory.cleanup)
        Path('release.properties').write_text('scm.tag=1.1.11\n')
        for artifact in ('target/checkout/target/kafka-connect-scylladb-1.1.11.jar',
                         'target/checkout/target/components/packages/scylladb-kafka-connect-scylladb-1.1.11.zip'):
            path = Path(artifact)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
        self.calls = []
        self.existing = None
        self.object = 'sha'
        self.commit = 'sha'
        self.remote = 'sha\trefs/tags/1.1.11\n'
        self.environment = patch.dict(os.environ, GITHUB_REPOSITORY='scylladb/kafka-connect-scylladb')
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.runner = patch.object(github_release, 'command', side_effect=self.run_command)
        self.runner.start()
        self.addCleanup(self.runner.stop)

    def run_command(self, *args, **kwargs):
        self.calls.append(args)
        if args[:2] == ('git', 'rev-parse'):
            identity = self.commit if args[-1].endswith('^{commit}') else self.object
            return CompletedProcess(args, 0, identity + '\n')
        if args[:2] == ('git', 'ls-remote'):
            if self.remote is None:
                raise subprocess.CalledProcessError(2, args, output='')
            return CompletedProcess(args, 0, self.remote)
        if args[:3] == ('gh', 'release', 'view'):
            return CompletedProcess(args, 1 if self.existing is None else 0,
                                    json.dumps(self.existing))
        return CompletedProcess(args, 0, '')

    def test_new_release_uses_exact_tag_generated_notes_and_verified_tag(self):
        github_release.publish()
        create = self.calls[-1]
        self.assertEqual(create[:4], ('gh', 'release', 'create', '1.1.11'))
        self.assertIn('--verify-tag', create)
        self.assertIn('--generate-notes', create)
        self.assertIn('--repo', create)
        self.assertFalse(any('describe' in call for call in self.calls))

    def test_existing_published_release_preserves_notes_and_replaces_artifacts(self):
        self.existing = {'tagName': '1.1.11', 'isDraft': False}
        github_release.publish()
        self.assertEqual(self.calls[-1][:4], ('gh', 'release', 'upload', '1.1.11'))
        self.assertIn('--clobber', self.calls[-1])
        self.assertNotIn('--generate-notes', self.calls[-1])

    def test_existing_draft_or_wrong_tag_fails_without_upload(self):
        for tag, draft in [('1.1.11', True), ('1.1.10', False)]:
            self.existing = {'tagName': tag, 'isDraft': draft}
            with self.assertRaisesRegex(RuntimeError, 'draft'):
                github_release.publish()
            self.assertEqual(self.calls[-1][:3], ('gh', 'release', 'view'))

    def test_missing_remote_tag_fails_before_release_api(self):
        self.remote = None
        with self.assertRaises(subprocess.CalledProcessError) as raised:
            github_release.publish()
        self.assertEqual(raised.exception.returncode, 2)
        self.assertEqual(self.calls[-1][:2], ('git', 'ls-remote'))
        self.assertFalse(any(call[0] == 'gh' for call in self.calls))

    def test_mismatched_remote_tag_fails_before_release_api(self):
        self.remote = 'other\trefs/tags/1.1.11\n'
        with self.assertRaisesRegex(RuntimeError, 'origin'):
            github_release.publish()
        self.assertEqual(self.calls[-1][:2], ('git', 'ls-remote'))
        self.assertFalse(any(call[0] == 'gh' for call in self.calls))

    def test_annotated_tags_compare_the_peeled_commit(self):
        self.object = 'object'
        self.remote = 'object\trefs/tags/1.1.11\nsha\trefs/tags/1.1.11^{}\n'
        github_release.publish()
        self.assertEqual(self.calls[-1][2], 'create')

    def test_missing_artifacts_fail_before_release_api(self):
        Path('target/checkout/target/kafka-connect-scylladb-1.1.11.jar').unlink()
        with self.assertRaisesRegex(RuntimeError, 'Missing release artifact'):
            github_release.publish()
        self.assertEqual(self.calls[-1][:2], ('git', 'ls-remote'))

    def test_recorded_tag_survives_maven_perform_cleanup(self):
        Path('release.properties').unlink()
        with patch.dict(os.environ, RELEASE_TAG='1.1.11'):
            github_release.publish()
        self.assertEqual(self.calls[-1][3], '1.1.11')

    def test_record_retains_exact_tag_object_and_commit_before_cleanup(self):
        self.object = 'a' * 40
        self.commit = 'b' * 40
        with patch.dict(os.environ, GITHUB_ENV='environment'):
            github_release.record()
        retained = Path('environment').read_text()
        self.assertIn('RELEASE_TAG=1.1.11\n', retained)
        self.assertIn('RELEASE_TAG_OBJECT=' + self.object + '\n', retained)
        self.assertIn('RELEASE_COMMIT=' + self.commit + '\n', retained)

    def test_changed_prepared_identity_fails_before_release_api(self):
        for field in ('RELEASE_TAG_OBJECT', 'RELEASE_COMMIT'):
            with self.subTest(field=field):
                self.calls.clear()
                retained = {'RELEASE_TAG_OBJECT': self.object, 'RELEASE_COMMIT': self.commit}
                retained[field] = 'different'
                with patch.dict(os.environ, retained):
                    with self.assertRaisesRegex(RuntimeError, 'changed after Maven prepare'):
                        github_release.publish()
                self.assertFalse(any(call[0] == 'gh' for call in self.calls))

    def test_tag_identity_rejects_missing_ambiguous_or_prefixed_tags(self):
        for properties in ['', 'scm.tag=v1.1.11\n', 'scm.tag=1.1.10\nscm.tag=1.1.11\n']:
            Path('release.properties').write_text(properties)
            with self.assertRaisesRegex(RuntimeError, 'scm.tag'):
                github_release.release_tag()


if __name__ == '__main__':
    unittest.main()
