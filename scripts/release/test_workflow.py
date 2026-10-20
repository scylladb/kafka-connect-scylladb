"""Check the release workflow's credential boundaries without release credentials."""

from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / '.github/workflows/release.yml').read_text()
        self.steps = re.split(r'^      - name: ', self.workflow, flags=re.M)[1:]

    def test_guard_survives_checkout_and_validates_before_key_import(self):
        text = self.workflow
        preserve = text.index('cp scripts/release/guard.py "$RUNNER_TEMP/release-guard.py"')
        checkout = text.index('git checkout')
        validate = text.index('python3 "$RELEASE_GUARD" check')
        credentials = text.index('gpg-private-key:')
        self.assertLess(preserve, checkout)
        self.assertLess(checkout, validate)
        self.assertLess(validate, credentials)

    def test_runner_paths_are_exported_after_runner_starts(self):
        job_header = self.workflow.split('    steps:', 1)[0]
        self.assertNotIn('${{ runner.', job_header)
        preserve_step = next(step for step in self.steps
                             if step.startswith('Preserve release guard\n'))
        self.assertIn('RELEASE_GUARD=$RUNNER_TEMP/release-guard.py" >> "$GITHUB_ENV"', preserve_step)
        self.assertIn('RELEASE_LOG_DIR=$RUNNER_TEMP/release-logs" >> "$GITHUB_ENV"', preserve_step)

    def test_all_four_maven_invocations_are_guarded(self):
        commands = [line.strip() for line in self.workflow.splitlines()
                    if re.search(r'\bmvn\b', line) and not line.lstrip().startswith('#')]
        # The fallback notification contains a prose reminder about release:prepare.
        commands = [line for line in commands if not line.startswith('**Do not re-run')]
        self.assertEqual(len(commands), 4)
        for goal in ('clean', 'release:clean', 'release:prepare', 'release:perform'):
            self.assertTrue(any('python3 "$RELEASE_GUARD" run mvn -B -ntp ' + goal
                                in command for command in commands), goal)

    def test_sanitized_log_upload_is_unconditional_and_nonfatal(self):
        upload = next(step for step in self.steps if step.startswith('Upload sanitized release logs\n'))
        warning = next(step for step in self.steps if step.startswith('Warn if sanitized log upload failed\n'))
        self.assertTrue(upload.startswith('Upload sanitized release logs\n'))
        self.assertIn('if: always()', upload)
        self.assertIn('continue-on-error: true', upload)
        self.assertIn('path: ${{ runner.temp }}/release-logs/*.log', upload)
        self.assertIn('if-no-files-found: ignore', upload)
        self.assertRegex(upload, r'uses: actions/upload-artifact@[0-9a-f]{40}')
        self.assertTrue(warning.startswith('Warn if sanitized log upload failed\n'))
        self.assertIn("steps.upload-logs.outcome == 'failure'", warning)
        self.assertIn('::warning::', warning)

    def test_inputs_are_not_interpolated_into_shell(self):
        for step in self.steps:
            if '        run:' in step:
                self.assertNotIn('${{ inputs.', step.split('        run:', 1)[1])

    def test_policy_check_has_read_only_permissions_and_pinned_actions(self):
        policy = (ROOT / '.github/workflows/release-credential-safety.yml').read_text()
        self.assertIn('contents: read', policy)
        self.assertNotIn(': write', policy)
        self.assertIn('branches: [master]', policy)
        self.assertIn('python3 scripts/release/guard.py check', policy)
        for action in re.findall(r'uses: (\S+)', policy):
            self.assertRegex(action, r'@[0-9a-f]{40}$')


if __name__ == '__main__':
    unittest.main()
