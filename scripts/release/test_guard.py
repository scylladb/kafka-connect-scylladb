"""Focused checks for release configuration and sanitized output."""

import base64
import io
import json
import os
import re
from pathlib import Path
import shutil
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import urllib.parse

import guard

ROOT = Path(__file__).resolve().parents[2]
CENTRAL_VERSION = re.compile(
    r"(<artifactId>central-publishing-maven-plugin</artifactId>\s*<version>)[^<]*(</version>)"
)


def with_central_version(pom, version):
    # Locate the pin by plugin, not by its current value, so dependency bumps can't
    # turn these tests into silent no-ops.
    updated, count = CENTRAL_VERSION.subn(lambda match: match[1] + version + match[2], pom)
    if count != 1:
        raise AssertionError(f"expected one central-publishing version pin, found {count}")
    return updated



def clean_guard_env():
    return {key: value for key, value in os.environ.items()
            if key not in guard.ENV_OPTIONS and key != "MAVEN_BASEDIR"}


class ReleaseTests(unittest.TestCase):
    def test_repository_defaults(self):
        guard.check_policy(ROOT)

    def test_run_redacts_and_appends_sanitized_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "mvn"
            binary.write_text(
                "#!/bin/sh\n"
                "printf 'stdout:%s\\n' \"$MAVEN_GPG_PASSPHRASE\"\n"
                "printf 'stderr:%s\\n' \"$MAVEN_GPG_PASSPHRASE\" >&2\n"
            )
            binary.chmod(0o755)
            logs = root / "logs"
            env = clean_guard_env()
            env.update(
                PATH=str(root) + os.pathsep + env.get("PATH", ""),
                RELEASE_LOG_DIR=str(logs),
                MAVEN_GPG_PASSPHRASE="dummy-secret",
            )
            command = [sys.executable, str(ROOT / "scripts/release/guard.py"),
                       "run", "mvn", "-B", "-ntp", "release:perform",
                       "-Drelease.autopublish=true"]
            for _ in range(2):
                result = subprocess.run(command, cwd=root, env=env,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "stdout:[REDACTED]\n")
                self.assertEqual(result.stderr, "stderr:[REDACTED]\n")
            self.assertEqual(
                (logs / "stdout.log").read_text(), "stdout:[REDACTED]\n" * 2
            )
            self.assertEqual(
                (logs / "stderr.log").read_text(), "stderr:[REDACTED]\n" * 2
            )

    def test_run_redacts_slash_escaped_json_password(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "mvn"
            binary.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' '{\"token\":\"sonatype-alpha\\/beta+gamma=\"}'\n"
            )
            binary.chmod(0o755)
            logs = root / "logs"
            env = clean_guard_env()
            env.update(
                PATH=str(root) + os.pathsep + env.get("PATH", ""),
                RELEASE_LOG_DIR=str(logs),
                SONATYPE_TOKEN_PASSWORD="sonatype-alpha/beta+gamma=",
            )
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/release/guard.py"),
                 "run", "mvn", "-B", "clean"],
                cwd=root, env=env, capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stdout, '{"token":[REDACTED]}\n')
            self.assertEqual((logs / "stdout.log").read_text(), result.stdout)

    def test_run_redacts_combined_html_and_json_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "mvn"
            binary.write_text(
                "#!/bin/sh\n"
                + r"printf '%s\n' 'alpha&#38;be\u0074a'" + "\n"
                + r"printf '%s\n' 'alpha\u0026apos;beta'" + "\n"
            )
            binary.chmod(0o755)
            logs = root / "logs"
            env = clean_guard_env()
            env.update(
                PATH=str(root) + os.pathsep + env.get("PATH", ""),
                RELEASE_LOG_DIR=str(logs),
                MAVEN_GPG_PASSPHRASE="alpha&beta",
                SONATYPE_TOKEN_PASSWORD="alpha'beta",
            )
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/release/guard.py"),
                 "run", "mvn", "-B", "clean"],
                cwd=root, env=env, capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stdout, "[REDACTED]\n[REDACTED]\n")
            self.assertEqual((logs / "stdout.log").read_text(), result.stdout)

    def test_maven_basedir_override_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            env = clean_guard_env()
            env["MAVEN_BASEDIR"] = directory
            for arguments in (("check",), ("run", "mvn", "-B", "clean")):
                with self.subTest(arguments=arguments):
                    result = subprocess.run(
                        [sys.executable, str(ROOT / "scripts/release/guard.py"), *arguments],
                        cwd=ROOT, env=env, capture_output=True, text=True,
                    )
                    self.assertEqual(result.returncode, 2)
                    self.assertNotIn(directory, result.stderr)

    def test_ancestor_maven_config_is_rejected_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            target = parent / "target"
            target.mkdir()
            shutil.copytree(ROOT / ".github", target / ".github")
            shutil.copy(ROOT / "pom.xml", target / "pom.xml")
            (parent / ".mvn").mkdir()
            (parent / ".mvn/maven.config").write_text("-X\n--log-file=raw.log\n")
            with self.assertRaisesRegex(guard.Unsafe, "ancestor Maven configuration"):
                guard.check_policy(target)
            # Maven ignores a regular file named .mvn and keeps searching.
            (target / ".mvn").write_text("not a directory")
            with self.assertRaisesRegex(guard.Unsafe, "ancestor Maven configuration"):
                guard.check_policy(target)
            binary = parent / "mvn"
            binary.write_text("#!/bin/sh\nprintf 'ran\\n'\n")
            binary.chmod(0o755)
            env = clean_guard_env()
            env["PATH"] = str(parent) + os.pathsep + env.get("PATH", "")
            for arguments in (("check",), ("run", "mvn", "-B", "clean")):
                with self.subTest(arguments=arguments):
                    result = subprocess.run(
                        [sys.executable, str(ROOT / "scripts/release/guard.py"), *arguments],
                        cwd=target, env=env, capture_output=True, text=True,
                    )
                    self.assertEqual(result.returncode, 2)
                    self.assertNotIn("ran", result.stdout)
                    self.assertNotIn("raw.log", result.stderr)

    def test_preimport_check_rejects_unsafe_environment_options(self):
        env = clean_guard_env()
        env["MAVEN_OPTS"] = "-Dgpg.bestPractices=false"
        env.pop("MAVEN_BASEDIR", None)
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/release/guard.py"), "check"],
            cwd=ROOT, env=env, capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("Selected release target fails", result.stderr)

    def test_launcher_glob_options_are_rejected(self):
        option = "-Dorg.slf4j.simpleLogger.[l]ogFile=raw.log"
        with self.assertRaises(guard.Unsafe):
            guard.check_text(option, jvm_options=True)
        for name in ("MAVEN_OPTS", "MAVEN_ARGS"):
            with self.subTest(name=name):
                env = clean_guard_env()
                env[name] = option
                result = subprocess.run(
                    [sys.executable, str(ROOT / "scripts/release/guard.py"), "check"],
                    cwd=ROOT, env=env, capture_output=True, text=True,
                )
                self.assertEqual(result.returncode, 2)

    def test_policy_rejection_explains_unsafe_option(self):
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/release/guard.py"),
             "run", "mvn", "-X", "clean"],
            cwd=ROOT, env=clean_guard_env(), capture_output=True, text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("Maven debug options are not allowed", result.stderr)

    def test_maven_rc_is_skipped_during_guarded_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "mvn"
            binary.write_text(
                "#!/bin/sh\n"
                "if [ -z \"$MAVEN_SKIP_RC\" ]; then . \"$TEST_MAVENRC\"; fi\n"
                "printf '%s\\n' \"$MAVEN_GPG_PASSPHRASE\"\n"
            )
            binary.chmod(0o755)
            rc = root / "mavenrc"
            rc.write_text('printf "%s\\n" "$MAVEN_GPG_PASSPHRASE" > "$TEST_RAW_LOG"\n')
            raw_log = root / "raw.log"
            env = clean_guard_env()
            env.update(PATH=str(root) + os.pathsep + env.get("PATH", ""),
                       TEST_MAVENRC=str(rc), TEST_RAW_LOG=str(raw_log),
                       MAVEN_GPG_PASSPHRASE="test-secret",
                       RELEASE_LOG_DIR=str(root / "logs"))
            env.pop("MAVEN_BASEDIR", None)
            env.pop("MAVEN_SKIP_RC", None)
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/release/guard.py"),
                 "run", "mvn", "-B", "clean"],
                cwd=root, env=env, capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 0)
            self.assertIn("[REDACTED]", result.stdout)
            self.assertFalse(raw_log.exists())

    def test_nul_encoded_maven_output_is_not_published(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "mvn"
            binary.write_text("#!/bin/sh\nprintf 't\\000o\\000k\\000e\\000n\\000'\n")
            binary.chmod(0o755)
            logs = root / "logs"
            env = clean_guard_env()
            env["PATH"] = str(root) + os.pathsep + env.get("PATH", "")
            env["RELEASE_LOG_DIR"] = str(logs)
            env.pop("MAVEN_BASEDIR", None)
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/release/guard.py"),
                 "run", "mvn", "-B", "clean"],
                cwd=root, env=env, capture_output=True,
            )
            self.assertEqual(result.returncode, 2)
            self.assertNotIn(b"t\0o\0k\0e\0n\0", result.stdout + result.stderr)
            self.assertFalse((logs / "stdout.log").exists())

    def test_unsafe_options(self):
        for value in (
            "-X",
            "-l raw.log",
            "-lraw.log",
            "-l=raw.log",
            "--log-file raw.log",
            "--log-file=raw.log",
            "-Dgpg.bestPractices=false",
            "-Dgpg.bestPractices=no",
            "-Dgpg.bestPractices=0",
            "-Dgpg.bestPractices=",
            "--define=gpg.bestPractices=maybe",
            "-define=gpg.bestPractices=false",
            "-define=javax.net.debug=all",
            "-define=http.authorization=dummy",
            "-define password=dummy",
            "-Djavax.net.debug=all",
            "-Djavax.net.debug=ssl:record:plaintext",
            "-Djavax.net.debug",
            '--define=javax.net.debug="all"',
            '-Dgpg.bestPractices="false"',
            "-Dgpg.bestPractices='false'",
            "-Dorg.slf4j.simpleLogger.logFile=raw.log",
            "-Dcredentials=dummy",
            "--define=service.credential=dummy",
            "-Duser.dir=/tmp/unchecked",
            "-Duser.home=/tmp/unchecked",
            "-Dmaven.conf=/tmp/unchecked",
            "-Dsun.stderr.encoding=UTF-16LE",
            "-Dsun.stdout.encoding=UTF-16LE",
            "-Dfile.encoding=UTF-16LE",
            "-Dorg.slf4j.simpleLogger.logFile",
            "--define=org.slf4j.simpleLogger.logFile",
            '-Darguments="-Dorg.slf4j.simpleLogger.logFile"',
            "-Dorg.slf4j.simpleLogger.logFile=SYSTEM.OUT",
            "-Dorg.slf4j.simpleLogger.logFile=System.out",
            "-Dorg.slf4j.simpleLogger.logFile=System.err",
            "-Dorg.slf4j.simpleLogger.logFile=System.out raw.log",
            "--debug",
            "--X",
            "--l=raw.log",
            "-debug",
            '-Dorg.slf4j.simpleLogger.log.org.apache.http="debug"',
            '-Dorg.slf4j.simpleLogger."logFile"=raw.log',
            "-Dorg.slf4j.simpleLogger.log.org.apache.http='trace'",
            '-Darguments="-X"',
            '-Darguments=-Xverify',
            "-Dgpg.passphrase=dummy",
            "-Dhttp.authorization=dummy",
            "--define=gpg.passphrase=dummy",
            "-D gpg.passphrase=dummy",
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=trace",
            '-Dorg.slf4j.simpleLogger.log.org.apache.maven="debug"',
            "-Dorg.slf4j.simpleLogger.defaultLogLevel='trace'",
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value)
        guard.check_text("mvn -B -ntp release:perform -DskipTests=true")
        guard.check_text("-Dgpg.bestPractices=true")
        guard.check_text("if [[ $? -lt 127 ]]", "Makefile", maven_options=False)

    def test_whitespace_before_maven_property_equals_is_rejected(self):
        for value in (
            "-Dgpg.bestPractices =false",
            "--D=gpg.bestPractices =false",
            "-Dpassword =secret",
            "--D=password =secret",
            "-Darguments =-X",
            "-DprepareVerifyArgs =-X",
            "-DpomFileName =alternate.xml",
            "-Drelease.autopublish =maybe",
            "--define= gpg.bestPractices=false",
            "--define= password=secret",
            "--define= arguments=-X",
            "--define= arguments=-funsafe.xml",
            "--define= pomFileName=alternate.xml",
            "--define= release.autopublish=maybe",
            "-Dgpg.bestPractices\r=false",
            "-Darguments\r=-funsafe.xml",
            "-Dpassword\n=secret",
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, maven_arguments=[value])
        for value in ("gpg.bestPractices =false", "password =secret",
                      "arguments =-X", "gpg.bestPractices\r=false"):
            with self.subTest(split=value), self.assertRaises(guard.Unsafe):
                guard.check_text("-D " + value, maven_arguments=["-D", value])
        with self.assertRaises(guard.Unsafe):
            guard.check_text('--define\n"gpg.bestPractices =false"',
                             maven_arguments=["--define", '"gpg.bestPractices =false"'])

    def test_interpolated_logger_level_is_rejected(self):
        for level in ("${level}", "${logger.level}"):
            value = "-Dorg.slf4j.simpleLogger.log.org.apache.maven=" + level
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text("-Dlevel=debug " + value,
                                 maven_arguments=["-Dlevel=debug", value])

    def test_complete_gpg_boolean_values(self):
        for value in (
            '-Dgpg.bestPractices="true false"',
            "-Dgpg.bestPractices='true false'",
            '"-Dgpg.bestPractices=true false"',
            '-Dgpg."bestPractices"="true false"',
            '-Dgpg.bestPractices="true "',
            '-Dgpg.bestPractices',
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, jvm_options=True)
        for value in (
            '-Dgpg.bestPractices=true',
        ):
            guard.check_text(value, jvm_options=True)
        for value in (
            '-Dgpg.bestPractices="true"',
            "-Dgpg.bestPractices='true'",
            '"-Dgpg.bestPractices=true"',
            '-Dgpg."bestPractices"="true"',
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, jvm_options=True)
        guard.check_text("-Dgpg.bestPractices")  # Maven CLI bare properties mean true.
        for name in ("MAVEN_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
            for value in ('-Dgpg.bestPractices="true false"', "-Dgpg.bestPractices"):
                with self.subTest(name=name, value=value), patch.dict(
                    os.environ, {name: value}, clear=True
                ), patch.object(
                    sys, "argv", ["guard.py", "run", "mvn", "validate"]
                ), patch.object(guard, "capture") as capture, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
        for name in guard.ENV_OPTIONS:
            with self.subTest(name=name), patch.dict(
                os.environ, {name: "-Dorg.slf4j.simpleLogger.logFile=raw.log"}, clear=True
            ), patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), patch.object(
                guard, "capture"
            ) as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                capture.assert_not_called()

    def test_direct_gpg_argument_boundaries(self):
        for value, expected in (("true false", 2), ("true ", 2), ("true", 0)):
            with self.subTest(value=value), patch.dict(os.environ, {}, clear=True), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "-Dgpg.bestPractices=" + value]
            ), patch.object(guard, "capture", return_value=0) as capture, patch(
                "sys.stderr", io.StringIO()
            ):
                self.assertEqual(guard.main(), expected)
                self.assertEqual(capture.called, expected == 0)

    def test_direct_gpg_aliases_and_nested_arguments(self):
        for option in ("-D", "-define=", "--define="):
            with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                argument = option + "gpg.bestPractices=true false"
                guard.check_text(argument, maven_arguments=[argument])
        with self.assertRaises(guard.Unsafe):
            guard.check_text("-D gpg.bestPractices=true false",
                             maven_arguments=["-D", "gpg.bestPractices=true false"])
        nested = "-Darguments=-Dgpg.bestPractices=true -DskipTests=true"
        guard.check_text(nested, maven_arguments=[nested])
        escaped = r"-DpreparationGoals=-Dsafe=foo\ -B-l raw.log verify"
        with self.assertRaises(guard.Unsafe):
            guard.check_text(escaped, maven_arguments=[escaped])

    def test_direct_debug_options(self):
        guard.check_text("-Xmx1g -Xms256m")
        for option in ("-Xq", "-debug"):
            with self.subTest(option=option), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", option]
            ), patch.object(guard, "capture") as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                capture.assert_not_called()

    def test_maven_argument_sources_reject_debug_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".mvn").mkdir()
            with patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "validate"]
            ), patch.object(guard, "capture", return_value=0) as capture, patch(
                "sys.stderr", io.StringIO()
            ):
                for value in ("-Xvalidate", '"-Xverify"'):
                    with patch.dict(os.environ, {"MAVEN_ARGS": value}, clear=True):
                        self.assertEqual(guard.main(), 2)
                        capture.assert_not_called()
                with patch.dict(os.environ, {}, clear=True), patch.object(
                    guard, "Path",
                    side_effect=lambda value: root / value if value == ".mvn" else Path(value)
                ):
                    (root / ".mvn/maven.config").write_text("-Xvalidate")
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                    (root / ".mvn/maven.config").write_text("-Dgpg.bestPractices=true false")
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                    (root / ".mvn/maven.config").write_text("-DargLine=-Xmx512m -XX:+UseG1GC")
                    self.assertEqual(guard.main(), 0)
                    (root / ".mvn/maven.config").unlink()
                    (root / ".mvn/jvm.config").write_text("-Dgpg.bestPractices")
                    self.assertEqual(guard.main(), 2)
                    (root / ".mvn/jvm.config").write_text("-Xmx1g")
                    self.assertEqual(guard.main(), 0)
                capture.reset_mock()
                with patch.dict(
                    os.environ, {"MAVEN_ARGS": "-DargLine='-Xmx512m -XX:+UseG1GC'"}, clear=True
                ):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                capture.reset_mock()
                with patch.dict(os.environ, {"MAVEN_OPTS": "-Xmx1g"}, clear=True):
                    self.assertEqual(guard.main(), 0)
                    capture.assert_called_once()
                with patch.dict(os.environ, {"MAVEN_OPTS": "@.mvn/extra.options"}, clear=True):
                    self.assertEqual(guard.main(), 2)

    def test_unwrapped_release_goals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            for goal in ("prepare", "perform"):
                for options in ("", "-B ", "-B -ntp "):
                    (root / "Makefile").write_text(f"\t$(MVNCMD) {options}release:{goal}\n")
                    with self.subTest(goal=goal, options=options), self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)

    def test_old_release_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for case, text, message in (
                ("bestPractices", pom.replace("<gpg.bestPractices>true</gpg.bestPractices>", ""),
                 "enable GPG best practices"),
                ("central 0.8.0", with_central_version(pom, "0.8.0"), "update Central publishing"),
                ("central 0.10.0", with_central_version(pom, "0.10.0"),
                 "update Central publishing"),
            ):
                (root / "pom.xml").write_text(text)
                with self.subTest(case=case), self.assertRaisesRegex(guard.Unsafe, message):
                    guard.check_policy(root)

    def test_ineligible_target_has_explicit_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pom = root / "pom.xml"
            pom.write_text((ROOT / "pom.xml").read_text())
            for content in (pom.read_text(), "<project>"):
                with self.subTest(malformed=content == "<project>"):
                    pom.write_text(content)
                    result = subprocess.run(
                        [sys.executable, str(ROOT / "scripts/release/guard.py"), "check"],
                        cwd=root, env=clean_guard_env(), capture_output=True, text=True,
                    )
                    self.assertEqual(result.returncode, 2)
                    self.assertIn("Selected release target fails", result.stderr)
                    self.assertNotIn("Traceback", result.stderr)

    def test_release_target_requires_guarded_workflow_and_log_upload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            workflow = root / ".github/workflows/release.yml"
            original = workflow.read_text()
            for marker, replacement in (("RELEASE_GUARD", "UNSAFE_GUARD"),
                                        ("if: always()", "if: success()")):
                with self.subTest(marker=marker):
                    self.assertIn(marker, original)
                    workflow.write_text(original.replace(marker, replacement))
                    with self.assertRaisesRegex(guard.Unsafe, "workflow sanity check"):
                        guard.check_policy(root)

    def test_release_target_requires_pinned_central_publishing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            central_plugin = (
                "            <plugin>\n"
                "                <groupId>org.sonatype.central</groupId>\n"
                "                <artifactId>central-publishing-maven-plugin</artifactId>\n"
                "                <version>0.12.0</version>\n"
                "            </plugin>\n"
            )
            self.assertIn(central_plugin, pom)
            (root / "pom.xml").write_text(pom.replace(central_plugin, ""))
            with self.assertRaisesRegex(guard.Unsafe, "pin Central publishing"):
                guard.check_policy(root)
            result = subprocess.run(
                [sys.executable, str(ROOT / "scripts/release/guard.py"), "check"],
                cwd=root, env=clean_guard_env(), capture_output=True, text=True,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn("pin Central publishing", result.stderr)
            (root / "pom.xml").write_text(pom.replace(
                "<version>0.12.0</version>", "<version></version>", 1
            ))
            with self.assertRaisesRegex(guard.Unsafe, "update Central publishing"):
                guard.check_policy(root)

    def test_alternate_maven_settings_are_rejected(self):
        for option in ("-s", "-gs", "-global-settings", "--s", "--gs",
                       "--settings", "--global-settings"):
            with self.subTest(option=option), self.assertRaisesRegex(
                guard.Unsafe, "alternate Maven settings"
            ):
                guard.check_text(option + " unsafe.xml", maven_arguments=[option, "unsafe.xml"])
            with self.subTest(nested=option), self.assertRaises(guard.Unsafe):
                guard.check_text(
                    "-Darguments=" + option + " unsafe.xml",
                    maven_arguments=["-Darguments=" + option + " unsafe.xml"],
                )
        for option in ("--s=unsafe.xml", "--gs=unsafe.xml",
                       "-global-settings=unsafe.xml"):
            with self.subTest(option=option), self.assertRaisesRegex(
                guard.Unsafe, "alternate Maven settings"
            ):
                guard.check_text(option, maven_arguments=[option])
        guard.check_text("--show-version", maven_arguments=["--show-version"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            for option in ("-s\nunsafe.xml\n", "--s=unsafe.xml\n", "--gs=unsafe.xml\n",
                           "-global-settings=unsafe.xml\n"):
                with self.subTest(config=option):
                    (root / ".mvn/maven.config").write_text(option)
                    with self.assertRaisesRegex(guard.Unsafe, "alternate Maven settings"):
                        guard.check_policy(root)
            (root / ".mvn/maven.config").unlink()
            workflow = root / ".github/workflows/release.yml"
            workflow.write_text(workflow.read_text() + "\n# curl -s is not a Maven setting\n")
            guard.check_policy(root)

    def test_combined_maven_options_are_rejected(self):
        for option in ("-B-X", "-B--settings", "-B-s", "-B-l",
                       "-B=-f", "-B=-X", "-B=-l", "--B=-f",
                       "--batch-mode=-f", "--batch-mode=-X",
                       "--batch-mode=--settings"):
            with self.subTest(direct=option), self.assertRaises(guard.Unsafe):
                guard.check_text(option + " unsafe.xml", maven_arguments=[option, "unsafe.xml"])
            with self.subTest(child=option), self.assertRaises(guard.Unsafe):
                guard.check_text(
                    "-Darguments=" + option + " unsafe.xml",
                    maven_arguments=["-Darguments=" + option + " unsafe.xml"],
                )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            for option in ("-B-X", "-B--settings unsafe.xml", "-B-l raw.log",
                           "-B=-f unsafe.xml", "--batch-mode=-f unsafe.xml"):
                with self.subTest(config=option):
                    (root / ".mvn/maven.config").write_text(option + "\n")
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
        for property_option in ("-Dsafe=-f", "--define=safe=-f"):
            with self.subTest(property_option=property_option):
                guard.check_text(property_option, maven_arguments=[property_option])

    def test_old_gpg_plugin_cannot_pass_target_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for version in ("1.6", "3.1.0", "${gpg.version}"):
                (root / "pom.xml").write_text(pom.replace("<version>3.2.8</version>",
                                                        f"<version>{version}</version>"))
                with self.subTest(version=version), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)
            (root / "pom.xml").write_text(pom.replace("<version>3.2.8</version>",
                                                    "<version>3.2.0</version>"))
            guard.check_policy(root)

    def test_pom_nested_release_debug_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text().replace(
                "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                "<arguments>-Xverify</arguments>",
            )
            (root / "pom.xml").write_text(pom)
            with self.assertRaises(guard.Unsafe):
                guard.check_policy(root)

    def test_pom_nested_release_property_cannot_hide_debug(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text().replace(
                "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                "<arguments>${release.extraArgs}</arguments>",
            ).replace("<properties>", "<properties><release.extraArgs>-Xverify</release.extraArgs>")
            (root / "pom.xml").write_text(pom)
            with self.assertRaises(guard.Unsafe):
                guard.check_policy(root)

    def test_release_child_argument_aliases_are_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for element, value in (
                ("prepareVerifyArgs", "-Xverify"),
                ("prepareVerifyArgs", "${release.extraArgs}"),
                ("goals", "deploy -Xverify"),
                ("preparationGoals", "verify -Xverify"),
                ("completionGoals", "deploy -f unsafe.xml"),
            ):
                with self.subTest(element=element, value=value):
                    (root / "pom.xml").write_text(pom.replace(
                        "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                        f"<{element}>{value}</{element}>",
                    ))
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
            for element in ("goals", "preparationGoals", "completionGoals"):
                with self.subTest(property=element):
                    (root / "pom.xml").write_text(pom.replace(
                        "<properties>",
                        f"<properties><{element}>deploy -f unsafe.xml</{element}>",
                        1,
                    ))
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
            (root / "pom.xml").write_text(pom.replace(
                "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                "<prepareVerifyArgs>-DskipTests=true</prepareVerifyArgs>",
            ))
            guard.check_policy(root)

    def test_release_pom_file_name_override_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for value in ("unsafe-pom.xml", "pom.xml", "${release.pom}"):
                with self.subTest(value=value):
                    (root / "pom.xml").write_text(pom.replace(
                        "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>",
                        f"<pomFileName>{value}</pomFileName>",
                    ))
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)
            (root / "pom.xml").write_text(pom.replace(
                "<properties>", "<properties><pomFileName>unsafe.xml</pomFileName>", 1
            ))
            with self.assertRaises(guard.Unsafe):
                guard.check_policy(root)
            (root / "pom.xml").write_text(pom)
            (root / ".mvn").mkdir()
            for option in ("-DpomFileName=unsafe-pom.xml", "--define=pomFileName=unsafe-pom.xml"):
                with self.subTest(option=option):
                    (root / ".mvn/maven.config").write_text(option)
                    with self.assertRaises(guard.Unsafe):
                        guard.check_policy(root)

    def test_release_child_properties_are_rejected_before_run(self):
        for option in (
            "-DpomFileName=unsafe-pom.xml",
            "-DprepareVerifyArgs=-Xverify",
            "-DprepareVerifyArgs=-f unsafe.xml",
            "-Darguments=--file=unsafe.xml",
            "-DprepareVerifyArgs=${release.extraArgs}",
            "-Darguments=-l raw.log",
            "-Dgoals=deploy -f unsafe.xml",
            "-DcompletionGoals=deploy -f unsafe.xml",
            "-Dgoals=deploy -Xverify",
            "-DpreparationGoals=verify -Xverify",
            "-DcompletionGoals=deploy -Xverify",
            "-D=arguments=-funsafe.xml",
            "--D=arguments=-funsafe.xml",
            "-D=gpg.bestPractices=false",
            "--D=gpg.bestPractices=false",
        ):
            with self.subTest(option=option), patch.dict(os.environ, {}, clear=True), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", option]
            ), patch.object(guard, "capture") as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                capture.assert_not_called()
        for option in ("-Darguments=-f unsafe.xml", "--define=prepareVerifyArgs=${extra}"):
            with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                guard.check_text(option)
        for name, value in (("arguments", "-funsafe.xml"),
                            ("goals", "-Xverify"),
                            ("release.autopublish", "maybe"),
                            ("gpg.bestPractices", "false")):
            with self.subTest(split_property=name), self.assertRaises(guard.Unsafe):
                guard.check_text("--D " + name + "=" + value,
                                 maven_arguments=["--D", name + "=" + value])
        guard.check_text("-Darguments=-DskipTests=true")
        guard.check_text("-DprepareVerifyArgs=-fae")

    def test_nested_release_properties_in_environment_and_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".mvn").mkdir()
            with patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), patch.object(
                guard, "capture"
            ) as capture, patch("sys.stderr", io.StringIO()):
                for name, option in (
                    ("MAVEN_ARGS", "-Darguments=-funsafe.xml"),
                    ("MAVEN_OPTS", "-DprepareVerifyArgs='-f unsafe.xml'"),
                    ("MAVEN_CONFIG", "-Darguments=${release.extraArgs}"),
                    ("MAVEN_CONFIG", "-D=gpg.bestPractices=false"),
                ):
                    with self.subTest(name=name), patch.dict(
                        os.environ, {name: option}, clear=True
                    ):
                        self.assertEqual(guard.main(), 2)
                        capture.assert_not_called()
                with patch.dict(os.environ, {}, clear=True), patch.object(
                    guard, "Path",
                    side_effect=lambda value: root / value if value == ".mvn" else Path(value)
                ):
                    for name, option in (
                        ("maven.config", "-DprepareVerifyArgs=--file unsafe.xml"),
                        ("maven.config", "-D=gpg.bestPractices=false"),
                        ("maven.config", "--D\narguments=-funsafe.xml\n"),
                        ("maven.config", "--D\ngoals=-Xverify\n"),
                        ("jvm.config", "-Darguments='-f unsafe.xml'"),
                    ):
                        with self.subTest(name=name):
                            (root / ".mvn" / name).write_text(option)
                            self.assertEqual(guard.main(), 2)
                            capture.assert_not_called()
                            (root / ".mvn" / name).unlink()

    def test_release_autopublish_property_must_be_boolean(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            (root / "pom.xml").write_text(pom.replace(
                "<release.autopublish>false</release.autopublish>",
                "<release.autopublish>false -Xverify</release.autopublish>",
            ))
            with self.assertRaises(guard.Unsafe):
                guard.check_policy(root)
            for option in ("-Drelease.autopublish=-Xverify", "-Drelease.autopublish=maybe",
                           "--define=release.autopublish=false -Xverify",
                           "--Drelease.autopublish=false -Xverify"):
                with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                    guard.check_text(option, maven_arguments=[option])

    def test_alternate_maven_pom_is_rejected_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            for option in ("--file=unsafe-pom.xml", "--file unsafe-pom.xml",
                           "-f unsafe-pom.xml", "-funsafe-pom.xml", "-ff.xml",
                           "--f=unsafe-pom.xml"):
                (root / ".mvn/maven.config").write_text(option)
                with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)

    def test_quoted_child_maven_flag_is_rejected_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            original = "<arguments>-Drelease.autopublish=${release.autopublish}</arguments>"
            for option in ('-"f"unsafe.xml', "-'f'unsafe.xml", '-"l"raw.log',
                           '-"X"verify', "-'X'verify", '-"debug"'):
                with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                    (root / "pom.xml").write_text(pom.replace(
                        original, f"<arguments>{option}</arguments>"
                    ))
                    guard.check_policy(root)

    def test_jvm_argument_file_is_rejected_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            (root / ".mvn/extra.options").write_text(
                "-Dorg.slf4j.simpleLogger.logFile=raw.log"
            )
            (root / ".mvn/jvm.config").write_text("@.mvn/extra.options")
            with self.assertRaises(guard.Unsafe):
                guard.check_policy(root)

    def test_jvm_vm_options_file_is_rejected_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            shutil.copy(ROOT / "pom.xml", root / "pom.xml")
            (root / ".mvn").mkdir()
            (root / ".mvn/extra.options").write_text(
                "-Dorg.slf4j.simpleLogger.logFile=raw.log"
            )
            (root / ".mvn/jvm.config").write_text(
                "-XX:VMOptionsFile=.mvn/extra.options"
            )
            with self.assertRaisesRegex(guard.Unsafe, "unverified JVM option files"):
                guard.check_policy(root)
            with self.assertRaisesRegex(guard.Unsafe, "unverified JVM option files"):
                guard.check_text("-XX:VMOptionsFile=extra.options", jvm_options=True)

    def test_inherited_gpg_configuration_is_rejected_before_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target"
            target.mkdir()
            shutil.copytree(ROOT / ".github", target / ".github")
            parent = (
                '<project xmlns="http://maven.apache.org/POM/4.0.0">'
                '<modelVersion>4.0.0</modelVersion><groupId>local</groupId>'
                '<artifactId>base</artifactId><version>1</version><packaging>pom</packaging>'
                '<build><plugins><plugin><groupId>org.apache.maven.plugins</groupId>'
                '<artifactId>maven-gpg-plugin</artifactId><configuration>'
                '<bestPractices>false</bestPractices></configuration></plugin></plugins></build>'
                '</project>'
            )
            (root / "pom.xml").write_text(parent)
            child = (ROOT / "pom.xml").read_text().replace(
                '<modelVersion>4.0.0</modelVersion>',
                '<modelVersion>4.0.0</modelVersion><parent>'
                '<groupId>local</groupId><artifactId>base</artifactId><version>1</version>'
                '<relativePath>../pom.xml</relativePath></parent>',
                1,
            )
            (target / "pom.xml").write_text(child)
            with self.assertRaisesRegex(guard.Unsafe, "unverified parent"):
                guard.check_policy(target)

    def test_disabled_gpg_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            for value in ("false", "no", "0", "", "true"):
                pom = (ROOT / "pom.xml").read_text().replace(
                    "<artifactId>maven-gpg-plugin</artifactId>",
                    "<artifactId>maven-gpg-plugin</artifactId>"
                    f"<configuration><bestPractices>{value}</bestPractices></configuration>",
                )
                (root / "pom.xml").write_text(pom)
                with self.subTest(value=value):
                    if value == "true":
                        guard.check_policy(root)
                    else:
                        with self.assertRaises(guard.Unsafe):
                            guard.check_policy(root)

    def test_disabled_gpg_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for value in ("false", "no", "0", ""):
                pom = (ROOT / "pom.xml").read_text().replace(
                    "<profiles>",
                    "<profiles><profile><id>unsafe</id><properties>"
                    f"<gpg.bestPractices>{value}</gpg.bestPractices></properties></profile>",
                )
                (root / "pom.xml").write_text(pom)
                with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)

    def test_newer_central_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / ".github", root / ".github")
            pom = (ROOT / "pom.xml").read_text()
            for version in ("0.11.0", "0.11.1", "0.12.0", "1.0.0"):
                (root / "pom.xml").write_text(with_central_version(pom, version))
                with self.subTest(version=version):
                    guard.check_policy(root)

    def test_option_locations_hide_values(self):
        for text in ("safe\ncurl -X POST secret", "safe\n-X", "safe\n--define\npassword=secret"):
            with patch("sys.stderr", new_callable=io.StringIO) as output:
                with self.assertRaises(guard.Unsafe):
                    guard.check_text(text, "Makefile")
                self.assertIn("Makefile:2", output.getvalue())
                self.assertNotIn("secret", output.getvalue())
        for text in ("-Dmaven.repo.local=/x/repo-f", "-Dfoo=a-s", "-fae -ff -fn -llr"):
            guard.check_text(text)

    def test_termination_signals_stop_maven(self):
        for signum in (signal.SIGTERM, signal.SIGHUP):
            with self.subTest(signum=signum), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ready = root / "ready"
                mvn = root / "mvn"
                mvn.write_text(f"#!{sys.executable}\nimport os, time\n"
                               f"open({str(ready) + '.tmp'!r}, 'w').write(str(os.getpid()))\n"
                               f"os.rename({str(ready) + '.tmp'!r}, {str(ready)!r})\n"
                               "time.sleep(30)\n")
                mvn.chmod(0o755)
                process = subprocess.Popen(
                    [sys.executable, str(ROOT / "scripts/release/guard.py"), "run", "mvn"],
                    env=dict(clean_guard_env(),
                             PATH=directory + os.pathsep + os.environ["PATH"]),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                try:
                    deadline = time.monotonic() + 5
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(ready.exists())
                    child = int(ready.read_text())
                    process.send_signal(signum)
                    process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 128 + signum)
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child, 0)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate()

    def test_encoded_and_multiline_credentials(self):
        env = {
            "MAVEN_GPG_PASSPHRASE": "first line\nsecond line+/=",
            "SONATYPE_TOKEN_USERNAME": "dummy-user",
            "SONATYPE_TOKEN_PASSWORD": "dummy-password",
            "SERVICE_CREDENTIALS": "dummy-credential",
        }
        for value in [*env.values(), "dummy-user:dummy-password"]:
            for encoded in (
                value,
                base64.b64encode(value.encode()).decode(),
                urllib.parse.quote(value, safe=""),
                urllib.parse.quote(value),
                urllib.parse.quote(urllib.parse.quote(value)),
                urllib.parse.quote(urllib.parse.quote(value), safe=""),
                urllib.parse.quote_plus(value),
            ):
                with self.subTest(encoded=encoded):
                    self.assertEqual(
                        b"[REDACTED]", guard.redact(encoded.encode(), guard.secret_forms(env))
                    )
        self.assertEqual(
            guard.redact(b'Authorization: Bearer unknown\n-Dpassword="a\nb"', []),
            b"Authorization: [REDACTED]\n-Dpassword=[REDACTED]",
        )
        self.assertEqual(
            guard.redact(b"Authorization:\nordinary log line\n", []),
            b"Authorization:\nordinary log line\n",
        )
        self.assertEqual(
            guard.redact(b"-Dcredentials=unregistered-value", []),
            b"-Dcredentials=[REDACTED]",
        )
        secret = "alpha~beta&gamma"
        encoded = "alpha%7Ebeta%26gamma".encode()
        self.assertEqual(guard.redact(encoded, guard.secret_forms({"TOKEN": secret})),
                         b"[REDACTED]")

    def test_html_escaped_credential_values(self):
        for secret, output in (
            ("a'b&c", b"a'b&amp;c"),
            ('alpha"<&beta', b'alpha"&lt;&amp;beta'),
            ("a'b&c", b"a&#x27;b&amp;c"),
        ):
            with self.subTest(secret=secret, output=output):
                self.assertEqual(
                    guard.redact(output, guard.secret_forms({"TOKEN": secret})),
                    b"[REDACTED]",
                )

    def test_serialized_credential_values(self):
        secret = 'alpha"é\'beta'
        for encoded in (
            json.dumps(secret),
            json.dumps(secret, ensure_ascii=False),
            json.dumps(secret, ensure_ascii=False)[1:-1],
            shlex.quote(secret),
        ):
            with self.subTest(encoded=encoded):
                self.assertEqual(
                    guard.redact(encoded.encode(), guard.secret_forms({"TOKEN": secret})),
                    b"[REDACTED]",
                )

    def test_decoded_percent_escapes_ignore_hex_case(self):
        forms = guard.secret_forms({"TOKEN": "alpha/beta"})
        for encoded in (
            b"alpha\\u00252fbeta",
            b"$'alpha\\x252fbeta'",
            b"alpha&#37;2fbeta",
        ):
            with self.subTest(encoded=encoded):
                self.assertEqual(guard.redact(encoded, forms), b"[REDACTED]")

        # Ordinary credential letters still require the original case.
        self.assertEqual(
            guard.redact(b"Alpha\\u00252fbeta", forms),
            b"Alpha\\u00252fbeta",
        )

    def test_unterminated_quote_around_credential_argument(self):
        for quote in (b'"', b"'"):
            with self.subTest(quote=quote):
                self.assertEqual(
                    guard.redact(b"prefix " + quote + b"-Dpassword=two words", []),
                    b"prefix " + quote + b"-Dpassword=[REDACTED]",
                )

    def test_mixed_html_and_json_credential_encodings(self):
        for secret, encoded in (
            ("alpha&beta", b"alpha&#38;be\\u0074a"),
            ("alpha'beta", b"alpha\\u0026apos;beta"),
        ):
            with self.subTest(encoded=encoded):
                self.assertEqual(
                    guard.redact(encoded, guard.secret_forms({"TOKEN": secret})),
                    b"[REDACTED]",
                )

    def test_ansi_escaped_unregistered_credential_field(self):
        for encoded in (
            b"$'Authoriz\\x61tion: Bearer unregistered-token'",
            b"$'{\"Authoriz\\x61tion\":\"Bearer unregistered-token\"}'",
            b"$'-Dpassw\\x6frd=two words'",
        ):
            with self.subTest(encoded=encoded):
                self.assertEqual(guard.redact(encoded, []), b"[REDACTED]")

    def test_mixed_percent_encoded_url_password(self):
        forms = guard.secret_forms({"SONATYPE_TOKEN_PASSWORD": "alpha/beta+gamma"})
        encoded = b"https://user:alpha%2Fbeta+gamma@example.org/"
        self.assertEqual(
            guard.redact(encoded, forms),
            b"https://user:[REDACTED]@example.org/",
        )

    def test_percent_escapes_after_other_decoding(self):
        forms = guard.secret_forms({"TOKEN": "alpha/beta+gamma"})
        for encoded in (
            b"alpha\\u00252Fbeta+gamma",
            b"alpha&#37;2Fbeta+gamma",
            b"$'alpha\\x252Fbeta+gamma'",
        ):
            with self.subTest(encoded=encoded):
                self.assertEqual(guard.redact(encoded, forms), b"[REDACTED]")

    def test_control_bytes_in_maven_property_names_are_rejected(self):
        for control in ("\x00", "\x1b", "\x1f"):
            option = "-Dgpg.bestPractices" + control + "=false"
            with self.subTest(control=ord(control)), self.assertRaises(guard.Unsafe):
                guard.check_text(option, maven_arguments=[option])

    def test_folded_authorization_header(self):
        encoded = b"Authorization: Bearer\r\n unregistered-token\r\nnext line"
        self.assertEqual(
            guard.redact(encoded, []),
            b"Authorization: [REDACTED]\r\nnext line",
        )

    def test_html_safe_json_credential_values(self):
        secret = "alpha'<beta"
        forms = guard.secret_forms({"TOKEN": secret})
        for encoded in (b'"alpha\\u0027\\u003cbeta"', b'"alpha\\u0027\\u003Cbeta"'):
            with self.subTest(encoded=encoded):
                self.assertEqual(guard.redact(encoded, forms), b"[REDACTED]")

    def test_mixed_literal_and_unicode_escaped_credential(self):
        secret = "é<&beta"
        forms = guard.secret_forms({"TOKEN": secret})
        for encoded in ("é\\u003c&beta", "é<\\u0026beta"):
            with self.subTest(encoded=encoded):
                self.assertEqual(guard.redact(encoded.encode(), forms), b"[REDACTED]")
        astral = "face😀value"
        self.assertEqual(
            guard.redact(b"face\\ud83d\\ude00value", guard.secret_forms({"TOKEN": astral})),
            b"[REDACTED]",
        )

    def test_html_and_json_escapes_share_output_buffer(self):
        secret = "alpha&beta"
        encoded = b"\\u0041" * 6 + b" alpha&#38;beta"
        self.assertEqual(
            guard.redact(encoded, guard.secret_forms({"TOKEN": secret})),
            b"\\u0041" * 6 + b" [REDACTED]",
        )

        secret = "dummy-secret"
        encoded = b"&amp;" * 20 + b" dumm&#121;-secret tail \\u0041"
        self.assertEqual(
            guard.redact(encoded, guard.secret_forms({"TOKEN": secret})),
            b"&amp;" * 20 + b" [REDACTED] tail \\u0041",
        )

    def test_mixed_json_string_escapes(self):
        cases = (
            ('alpha\tbeta"gamma', b'alpha\\u0009beta\\"gamma'),
            ('alpha/"beta', b'"alpha\\/\\u0022beta"'),
        )
        for secret, encoded in cases:
            with self.subTest(secret=secret):
                self.assertEqual(
                    guard.redact(
                        encoded,
                        guard.secret_forms({"MAVEN_GPG_PASSPHRASE": secret}),
                    ),
                    b'"[REDACTED]"' if encoded.startswith(b'"') else b'[REDACTED]',
                )

    def test_json_escaped_unregistered_credential_property(self):
        encoded = b'prefix -Dpassword=\\"two words\\" suffix'
        sanitized = guard.redact(encoded, [])
        self.assertEqual(sanitized, b'prefix -Dpassword=[REDACTED] suffix')

    def test_unterminated_quoted_credential_property(self):
        for quote in (b'"', b"'"):
            with self.subTest(quote=quote):
                encoded = b'prefix -Dpassword=' + quote + b'two words'
                self.assertEqual(
                    guard.redact(encoded, []),
                    b'prefix -Dpassword=[REDACTED]',
                )

    def test_html_escaped_unregistered_credential_fields(self):
        cases = (
            b'{&quot;Authorization&quot;: &quot;Bearer unregistered-token&quot;}',
            b'prefix -Dpassword=&quot;two words&quot; suffix',
        )
        for encoded in cases:
            with self.subTest(encoded=encoded):
                sanitized = guard.redact(encoded, [])
                self.assertIn(b'[REDACTED]', sanitized)
                self.assertNotIn(b'unregistered-token', sanitized)
                self.assertNotIn(b'two words', sanitized)

    def test_named_and_numeric_html_credential_values(self):
        secret = "alphaé&beta"
        forms = guard.secret_forms({"TOKEN": secret})
        for encoded in (
            b"alpha&eacute;&amp;beta",
            b"alpha&#233;&amp;beta",
            b"alpha&#xE9;&beta",
            b"alpha&#xE9;&#38;beta",
        ):
            with self.subTest(encoded=encoded):
                self.assertEqual(guard.redact(encoded, forms), b"[REDACTED]")
        self.assertEqual(
            guard.redact(b"before alpha&eacute;&amp;beta after", forms),
            b"before [REDACTED] after",
        )
        # Escaped ANSI tokens are conservatively suppressed, even when benign.
        self.assertEqual(
            guard.redact(b"alpha&eacute;&amp;beta $'ordinary\\tmessage'", forms),
            b"[REDACTED] [REDACTED]",
        )

    def test_bash_xtrace_credential_value(self):
        secret = "alpha'beta"
        result = subprocess.run(
            ["bash", "-c", "set -x; printf '%s\\n' \"$TOKEN\""],
            env={"TOKEN": secret}, capture_output=True, check=True,
        )
        forms = guard.secret_forms({"TOKEN": secret})
        self.assertIn(b"[REDACTED]", guard.redact(result.stderr, forms))
        self.assertNotIn(b"alpha", guard.redact(result.stderr, forms))

    def test_bash_ansi_credential_value(self):
        secret = "alpha'beta\tgamma"
        self.assertEqual(
            guard.redact(b"$'alpha\\'beta\\tgamma'", guard.secret_forms({"TOKEN": secret})),
            b"[REDACTED]",
        )
        for secret, encoded in (
            ("alpha\abeta", b"$'alpha\\abeta'"),
            ("alpha\bbeta", b"$'alpha\\bbeta'"),
            ("alpha\x1bbeta", b"$'alpha\\ebeta'"),
            ("alpha\x1bbeta", b"$'alpha\\Ebeta'"),
            ("alpha\vbeta", b"$'alpha\\vbeta'"),
            ("alpha\abeta", b"$'alpha\\007beta'"),
            ("alphaébeta", b"$'alpha\\303\\251beta'"),
            ("alphaé\tbeta", b"$'alpha\\303\\251\\tbeta'"),
            ("alphaé\tbeta", b"$'alpha\\xc3\\xa9\\011beta'"),
            ("alpha\x7fbeta", b"$'alpha\\c?beta'"),
            ("alpha\\q\tbeta", b"$'alpha\\q\\tbeta'"),
            ("alpha\x1csecret", b"$'alpha\\c\\\\\\x73ecret'"),
        ):
            with self.subTest(encoded=encoded):
                self.assertEqual(
                    guard.redact(encoded, guard.secret_forms({"TOKEN": secret})),
                    b"[REDACTED]",
                )
        for secret in ("alpha\x01\tbeta", "alphaé\x1bbeta", "alphaé\tbeta",
                       "alpha\tbeta$"):
            for locale in ("C", "C.UTF-8"):
                with self.subTest(secret=secret, locale=locale):
                    rendered = subprocess.run(
                        ["bash", "-c", "printf %q \"$TOKEN\""],
                        env={"TOKEN": secret, "LC_ALL": locale},
                        capture_output=True, check=True,
                    ).stdout
                    self.assertEqual(
                        guard.redact(rendered, guard.secret_forms({"TOKEN": secret})),
                        b"[REDACTED]",
                    )

    def test_quoted_gpg_boolean_is_rejected(self):
        for value in ('-Dgpg.bestPractices="true"',
                      "-Dgpg.bestPractices='true'", '-D"gpg.bestPractices=true"',
                      '-D" gpg.bestPractices =false"',
                      '-Dgpg.bestPractices" "=false',
                      "--define=' gpg.bestPractices =false'"):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, maven_arguments=[value])
            with self.subTest(jvm=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, jvm_options=True)
        guard.check_text("-Dgpg.bestPractices=true",
                         maven_arguments=["-Dgpg.bestPractices=true"])

    def test_quoted_child_property_is_checked(self):
        for value in ('-D"arguments=-DskipTests=true"',
                      '-D"prepareVerifyArgs=-DskipTests=true"'):
            with self.subTest(value=value):
                guard.check_text(value, maven_arguments=[value])
        for value in ('-D"arguments=-funsafe.xml"',
                      '-D"prepareVerifyArgs=-Xverify"'):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, maven_arguments=[value])

    def test_quoted_credential_arguments(self):
        for quote in (b"'", b'"'):
            for option in (b"-Dpassword=", b"-Dpassword =", b"-D=password=",
                           b"--D=password=",
                           b"--define=token=", b"--define secret=",
                           b"-define=token=", b"-define secret="):
                with self.subTest(quote=quote, option=option):
                    result = guard.redact(quote + option + b"two words\nlast" + quote, [])
                    self.assertNotIn(b"words", result)
                    self.assertNotIn(b"last", result)
                    self.assertIn(b"[REDACTED]", result)

    def test_unquoted_credential_masks_remaining_stream(self):
        for option in (b"-Dpassword=", b"--define=token="):
            with self.subTest(option=option):
                result = guard.redact(option + b"two words tail\n[INFO] next line", [])
                self.assertNotIn(b"words", result)
                self.assertNotIn(b"tail", result)
                self.assertEqual(result, option + b"[REDACTED]")

    def test_unknown_unquoted_multiline_credentials(self):
        for separator in (b"\n", b"\r", b"\r\n"):
            for option in (b"-Dpassword=", b"--define=token="):
                with self.subTest(separator=separator, option=option):
                    payload = (b"safe prefix " + option + b"first" + separator
                               + b"private-tail" + separator + b"[INFO] ambiguous tail")
                    self.assertEqual(
                        guard.redact(payload, []), b"safe prefix " + option + b"[REDACTED]"
                    )

    def test_quoted_multiline_credentials_preserve_following_diagnostics(self):
        for quote in (b"'", b'"'):
            with self.subTest(quote=quote):
                self.assertEqual(
                    guard.redact(b"-Dpassword=" + quote + b"first\nprivate-tail"
                                 + quote + b"\n[INFO] next line", []),
                    b"-Dpassword=[REDACTED]\n[INFO] next line",
                )

    def test_adjacent_shell_credential_fragments_mask_stream(self):
        for value in (
            b'"first"private-tail', b"'first'private-tail",
            b'"first"second"third"fourth', b"'first'\"second\"'third'",
            b'"first"\\ private-tail', b'"first""second"',
            b'"first"unfinished"tail',
        ):
            for prefix in (b'-Dpassword=', b'"-Dpassword='):
                with self.subTest(value=value, prefix=prefix):
                    sanitized = guard.redact(prefix + value + b"\n[INFO] ambiguous", [])
                    self.assertNotIn(b"tail", sanitized)
                    self.assertNotIn(b"second", sanitized)
                    self.assertNotIn(b"ambiguous", sanitized)
                    self.assertIn(b"[REDACTED]", sanitized)
        self.assertEqual(guard.redact(b'-Dpassword="first" diagnostic', []),
                         b'-Dpassword=[REDACTED] diagnostic')

    def test_repeated_composed_decoders_map_original_bytes(self):
        # Each name needs three/four changing passes, in different orders.
        names = (
            b'Authoriz%26%2392%3Bu0061tion',  # percent -> HTML -> JSON
            b'Authoriz%2526%252392%253Bu0061tion',
            b'Authoriz\\u002526%23x%36%31%3Btion',  # JSON -> percent -> HTML
            b'Authoriz&#37;5Cu0061tion',  # HTML -> percent -> JSON
            b'Authoriz&#38;#37;5Cu0061tion',
            b'Authoriz\\u0026#37;26%2392%3Bu0061tion',
        )
        for name in names:
            with self.subTest(name=name):
                self.assertEqual(
                    guard.redact(b"before " + name + b": Bearer private-tail\nnext", []),
                    b"before " + name + b": [REDACTED]\nnext",
                )
        for encoded in (b'alpha%26%2392%3Bu0026beta',
                        b'alpha%2526%252392%253Bu0026beta',
                        b'alpha&#38;#37;5Cu0026beta'):
            with self.subTest(encoded=encoded):
                self.assertEqual(guard.redact(b"before " + encoded + b" after",
                                             guard.secret_forms({"TOKEN": "alpha&beta"})),
                                 b"before [REDACTED] after")

    def test_decoding_budgets_fail_closed(self):
        cases = (
            ("DECODE_DEPTH", 2, b'Authoriz%26%2392%3Bu0061tion: private-tail'),
            ("DECODE_STATES", 2, b'%26%2392%3Bu0061'),
            ("DECODE_WORK", 8, b'%41' * 4),
            ("DECODE_BYTES", 8, b'&nLt;' * 2),  # entities can expand
            ("DECODE_TEMP_BYTES", 8, b'%41'),
            ("DECODE_RECORDS", 2, b'%41' * 3),
        )
        for name, limit, payload in cases:
            with self.subTest(name=name), patch.object(guard, name, limit):
                with self.assertRaises(guard.Unsafe):
                    guard.redact(payload, [])
        # Default depth cap also stops short, adversarial nested inputs.
        with self.assertRaises(guard.Unsafe):
            guard.redact(b'&' + b'amp;' * 20 + b'quot;', [])

    def test_composition_limits_are_aggregate_and_depth_boundary_is_complete(self):
        for name, limit, payload in (
            ("DECODE_RECORDS", 1, b'%2541'),  # One replacement in each pass.
            ("DECODE_BYTES", 32, b'%41&amp;'),  # Competing first-stage views.
            ("DECODE_WORK", 80, b'%41&amp;'),
        ):
            with self.subTest(name=name), patch.object(guard, name, limit):
                with self.assertRaises(guard.Unsafe):
                    guard.redact(payload, [])
        # Eight changing passes are accepted if all remaining edges are inert.
        at_limit = b'Authoriz%' + b'25' * 7 + b'61tion: private-tail'
        self.assertEqual(guard.redact(at_limit, []),
                         at_limit.replace(b'private-tail', b'[REDACTED]'))
        with self.assertRaises(guard.Unsafe):
            guard.redact(at_limit.replace(b'%25', b'%2525', 1), [])

    def test_composition_preflight_leaves_standalone_allocations_unchanged(self):
        # Standalone HTML decoding still runs first, with its original limits.
        # The composition cap must prevent *another* decoder allocation.
        decoder_calls = []
        previous_profile = sys.getprofile()

        def profile(frame, event, arg):
            if event == "call" and frame.f_code.co_name == "decode_entities":
                decoder_calls.append(event)

        try:
            sys.setprofile(profile)
            with patch.object(guard, "DECODE_BYTES", 4096):
                with self.assertRaises(guard.Unsafe):
                    guard.redact(b'&nLt;' * 10000, [])
        finally:
            sys.setprofile(previous_profile)
        self.assertEqual(decoder_calls, [])

    def test_decoding_exhaustion_withholds_both_streams_and_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            logs = Path(directory) / "logs"
            command = [sys.executable, "-c",
                       "import os; os.write(1, b'safe first stream'); "
                       "os.write(2, b'Authoriz%26%2392%3Bu0061tion: private-tail')"]
            with patch.object(guard, "DECODE_DEPTH", 2), \
                    patch.object(guard.sys, "stdout", io.TextIOWrapper(io.BytesIO())), \
                    patch.object(guard.sys, "stderr", io.TextIOWrapper(io.BytesIO())):
                with self.assertRaises(guard.Unsafe):
                    guard.capture(command, os.environ, [], logs)
                self.assertEqual(guard.sys.stdout.buffer.getvalue(), b"")
                self.assertEqual(guard.sys.stderr.buffer.getvalue(), b"")
            self.assertFalse(logs.exists())

    def test_percent_first_combined_decoding(self):
        forms = guard.secret_forms({"TOKEN": "alpha&beta"})
        for encoded in (b"alpha%26%2338%3Bbeta", b"alpha%5Cu0026be%5Cu0074a"):
            with self.subTest(encoded=encoded):
                self.assertEqual(
                    guard.redact(b"before %41 " + encoded + b" after", forms),
                    b"before %41 [REDACTED] after",
                )
        for encoded in (
            b"-Dpassword=%26quot%3Bfirst%0Aprivate-tail%26quot%3B",
            b"-Dpassword=%5Cu0022first%5Cnprivate-tail%5Cu0022",
        ):
            with self.subTest(encoded=encoded):
                self.assertEqual(
                    guard.redact(b"before " + encoded, []),
                    b"before -Dpassword=[REDACTED]",
                )

    def test_percent_first_encoded_property_names_and_quoted_boundaries(self):
        for encoded in (
            b"%2DDpass%26%23119%3Bord%3D%26quot%3Bfirst%0Atail%26quot%3B",
            b"%2DDpass%5Cu0077ord%3D%5Cu0022first%5Cntail%5Cu0022",
        ):
            with self.subTest(encoded=encoded):
                self.assertEqual(
                    guard.redact(b"before " + encoded + b" after", []),
                    b"before " + encoded.split(b"%3D", 1)[0] + b"%3D[REDACTED] after",
                )

    def test_fresh_leaks_are_masked_in_console_and_log_artifacts(self):
        cases = (
            (b"%24%27%5Cx41uthorization%3A%20Bearer%20unregistered-token%27", b"[REDACTED]"),
            (b"$'Authoriz\\x26#97;tion: Bearer unregistered-token'", b"[REDACTED]"),
            (b'-Dpassword="first"second"third"private-tail', b'-Dpassword=[REDACTED]'),
            (b'Authoriz%26%2392%3Bu0061tion: Bearer private-tail\nnext',
             b'Authoriz%26%2392%3Bu0061tion: [REDACTED]\nnext'),
            (b'Authoriz%2526%252392%253Bu0061tion: Bearer private-tail',
             b'Authoriz%2526%252392%253Bu0061tion: [REDACTED]'),
            (b"safe -Dpassword=first\nprivate-tail\n[INFO] ambiguous", b"safe -Dpassword=[REDACTED]"),
            (b"before alpha%26%2338%3Bbeta after\n", b"before [REDACTED] after\n"),
            (b"before alpha%5Cu0026be%5Cu0074a after\n", b"before [REDACTED] after\n"),
        )
        for payload, expected in cases:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as directory:
                command = [sys.executable, "-c",
                           f"import os; os.write(1, {payload!r}); os.write(2, {payload!r}); "
                           "raise SystemExit(17)"]
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    f"raise SystemExit(guard.capture({command!r}, os.environ, "
                    f"guard.secret_forms({{'TOKEN': 'alpha&beta'}}), Path({directory!r})))"
                )
                result = subprocess.run(
                    [sys.executable, "-c", script], capture_output=True,
                    cwd=ROOT / "scripts/release", check=False,
                )
                self.assertEqual(result.returncode, 17)
                for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                    self.assertEqual(console, expected)
                    self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), expected)

    def test_repeated_property_prefixes_finish_promptly(self):
        import time

        started = time.monotonic()
        for payload in (b"-Daaaa" * 20000, b"-Da-D" * 12000):
            guard.redact(payload, [])
        self.assertLess(time.monotonic() - started, 5)

    def test_escaped_quotes_in_credential_arguments(self):
        for quote in (b"'", b'"'):
            secret = b"alpha" + b"\\" + quote + b"beta gamma"
            for value in (
                b"-Dpassword=" + quote + secret + quote,
                quote + b"-Dpassword=" + secret + quote,
            ):
                with self.subTest(value=value):
                    result = guard.redact(value, [])
                    self.assertNotIn(b"beta", result)
                    self.assertNotIn(b"gamma", result)
                    self.assertIn(b"[REDACTED]", result)

    def test_maven_equals_prefixed_credential_arguments(self):
        for option in (b"-D=password=", b"--D=password=", b"-Dpassword =",
                       b"--D=password =", b"-Dpassword\n=",
                       b"-Dpassword= ", b"--D=password= ",
                       b"--define=password= "):
            for value in (option + b"unregistered-value",
                          option + b'"unregistered-value"',
                          b'"' + option + b"unregistered-value" + b'"'):
                with self.subTest(value=value):
                    result = guard.redact(value, [])
                    self.assertNotIn(b"unregistered-value", result)
                    self.assertIn(b"[REDACTED]", result)

    def test_secrets_matching_credential_markers(self):
        for marker, output in (
            ("password", b"-Ddb.password=unregistered-value"),
            ("Authorization", b"Authorization: Bearer unregistered-value"),
        ):
            with self.subTest(marker=marker):
                result = guard.redact(output, guard.secret_forms({"TOKEN": marker}))
                self.assertNotIn(b"unregistered-value", result)
                self.assertNotIn(marker.encode(), result)

    def test_overlapping_structured_and_literal_secrets(self):
        for secret in ("prefix-Authorization: suffix", "prefix-Dpassword=value tail"):
            with self.subTest(secret=secret):
                result = guard.redact(secret.encode(), guard.secret_forms({"TOKEN": secret}))
                self.assertEqual(result, b"[REDACTED]")
        self.assertEqual(guard.redact(b"ababa", [b"aba"]), b"[REDACTED]")

    def test_repeated_ansi_quoted_secrets_use_bounded_memory(self):
        import tracemalloc

        payload = b"$'a'" * 65536
        forms = guard.secret_forms({"TOKEN": "a"})
        tracemalloc.start()
        try:
            sanitized = guard.redact(payload, forms)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()
        self.assertEqual(sanitized, b"[REDACTED]")
        self.assertLess(peak, 16 * 1024 * 1024)

    def test_ansi_composition_exact_reviewer_payloads(self):
        cases = (
            (b"%24%27%5Cx41uthorization%3A%20Bearer%20unregistered-token%27", []),
            (b"%24%27%5Cx61lpha-secret%27", guard.secret_forms({"TOKEN": "alpha-secret"})),
            (rb"$'Authoriz\x26#97;tion: Bearer unregistered-token'", []),
        )
        for payload, forms in cases:
            with self.subTest(payload=payload):
                self.assertEqual(guard.redact(payload, forms), b"[REDACTED]")

    def test_ansi_composes_in_both_orders_with_existing_formats(self):
        def ansi(value):
            return b"$'" + b"".join(b"\\x%02x" % byte for byte in value) + b"'"

        encoders = (
            lambda value: urllib.parse.quote_from_bytes(value, safe="").encode(),
            lambda value: b"".join(b"&#x%x;" % byte for byte in value),
            lambda value: b"".join(b"\\u%04x" % byte for byte in value),
        )
        for value, forms in (
            (b"Authorization: Bearer unregistered-token", []),
            (b"alpha-secret", guard.secret_forms({"TOKEN": "alpha-secret"})),
        ):
            for encode in encoders:
                for payload in (encode(ansi(value)), ansi(encode(value)),
                                encode(encode(ansi(value))),
                                encode(ansi(encode(value))), ansi(ansi(encode(value)))):
                    with self.subTest(value=value, payload=payload):
                        self.assertEqual(guard.redact(payload, forms), b"[REDACTED]")

    def test_ansi_fallback_original_offsets_and_stray_openers(self):
        tokens = (
            b"$'\\x61lpha-secret'",
            b"%24%27%5Cx61lpha-secret%27",
            b"&#36;&#39;\\x61lpha-secret&#39;",
            b"\\u0024\\u0027\\x61lpha-secret\\u0027",
            b"%2524%2527%255Cx61lpha-secret%2527",
        )
        for token in tokens:
            with self.subTest(token=token):
                # Non-ASCII prefix and changing bytes outside the token must
                # retain their original spelling and offsets across every map.
                prefix = b"\xc3\xa9 %41 &#66; \\u0043 "
                suffix = b" %44 &#69; \\u0046 tail"
                self.assertEqual(guard.redact(prefix + token + suffix, []),
                                 prefix + b"[REDACTED]" + suffix)
        stray = b"$'stray $'\\x61lpha-secret' tail"
        encoded_stray = urllib.parse.quote_from_bytes(stray, safe="").encode()
        self.assertEqual(guard.redact(encoded_stray, []),
                         b"%24%27stray%20[REDACTED]%20tail")
        # JSON can turn the escaped backslash into a quote escape. Withhold
        # the resulting unterminated token, including its ambiguous suffix.
        self.assertEqual(guard.redact(b"before $'\\\\' after", []),
                         b"before [REDACTED]")
        self.assertEqual(guard.redact(b"$'stray $'benign' tail", []),
                         b"$'stray $'benign' tail")

    def test_ansi_fallback_masks_benign_changed_tokens_only(self):
        for token in (b"$'benign\\ntext'", b"$'\\x41'", b"$'\\101'", b"$'\\cA'",
                      b"$'\\u0041'", b"$'\\U00000041'", b"$'\\\''"):
            with self.subTest(token=token):
                self.assertEqual(guard.redact(b"before " + token + b" after", []),
                                 b"before [REDACTED] after")
        for token in (b"$'benign'", b"$'\\q'", b"$'\\x'", b"$'\\uD800'", b"$'stray "):
            with self.subTest(token=token):
                self.assertEqual(guard.redact(token, []), token)
        self.assertEqual(guard.redact(b"before $'\\x41", []), b"before [REDACTED]")

    def test_repeated_escaped_ansi_tokens_use_bounded_memory(self):
        import tracemalloc

        for token in (b"$'\\x41'", b"%24%27%5Cx41%27"):
            with self.subTest(token=token):
                payload = (token + b" ") * 4096
                tracemalloc.start()
                try:
                    sanitized = guard.redact(payload, [])
                    peak = tracemalloc.get_traced_memory()[1]
                finally:
                    tracemalloc.stop()
                self.assertEqual(sanitized, b"[REDACTED] " * 4096)
                self.assertLess(peak, 16 * 1024 * 1024)

    def test_ansi_scans_share_work_budget_and_capture_withholds_output(self):
        payload = b"$'\\x41' " * 8
        # Composition scans cost 9*n here (JSON has no candidates).
        # ANSI scanning then exceeds this shared budget; no fresh budget.
        with patch.object(guard, "DECODE_WORK", 10 * len(payload)):
            with self.assertRaisesRegex(guard.Unsafe, "work budget"):
                guard.redact(payload, [])
        encoded = b"%24%27%5Cx41%27"
        # Both traversal views fit, but their deferred ANSI scans share work.
        with patch.object(guard, "DECODE_WORK", 10 * (len(encoded) + len(b"$'\\x41'"))):
            with self.assertRaisesRegex(guard.Unsafe, "work budget"):
                guard.redact(encoded, [])
        with patch.object(guard, "DECODE_DEPTH", 0):
            with self.assertRaisesRegex(guard.Unsafe, "depth or state"):
                guard.redact(encoded, [])
        with tempfile.TemporaryDirectory() as directory:
            logs = Path(directory) / "logs"
            command = [sys.executable, "-c",
                       f"import os; os.write(1, b'safe first stream'); os.write(2, {payload!r})"]
            with patch.object(guard, "DECODE_WORK", 10 * len(payload)), \
                    patch.object(guard.sys, "stdout", io.TextIOWrapper(io.BytesIO())), \
                    patch.object(guard.sys, "stderr", io.TextIOWrapper(io.BytesIO())):
                with self.assertRaisesRegex(guard.Unsafe, "work budget"):
                    guard.capture(command, os.environ, [], logs)
                self.assertEqual(guard.sys.stdout.buffer.getvalue(), b"")
                self.assertEqual(guard.sys.stderr.buffer.getvalue(), b"")
            self.assertFalse(logs.exists())

    def test_stray_ansi_opener_cannot_hide_later_credential(self):
        forms = guard.secret_forms({"TOKEN": "alpha\tbeta"})
        output = b"$'stray $'alpha\\x09beta' tail"
        sanitized = guard.redact(output, forms)
        self.assertEqual(sanitized, b"$'stray [REDACTED] tail")
        self.assertNotIn(b"alpha\\x09beta", sanitized)

    def test_unterminated_ansi_credential_is_redacted(self):
        forms = guard.secret_forms({"TOKEN": "alpha\tbeta"})
        self.assertEqual(guard.redact(b"$'alpha\\x09beta", forms), b"[REDACTED]")

    def test_quoted_authorization_headers(self):
        for header in (
            b'Authorization: Bearer unknown',
            b'"Authorization": "Bearer unknown"',
            b"'Proxy-Authorization': 'Basic unknown'",
        ):
            with self.subTest(header=header):
                self.assertEqual(
                    guard.redact(header, []), header.split(b":", 1)[0] + b": [REDACTED]"
                )
        escaped = b'{\\"Authorization\\": \\"Bearer unregistered-token\\"}'
        sanitized = guard.redact(escaped, [])
        self.assertIn(b"[REDACTED]", sanitized)
        self.assertNotIn(b"unregistered-token", sanitized)
        for escaped_key in (
            b'{"Authoriz\\u0061tion":"Bearer unregistered-token"}',
            b'{"Proxy-Authoriz\\u0061tion":"Basic unregistered-token"}',
        ):
            with self.subTest(escaped_key=escaped_key):
                sanitized = guard.redact(escaped_key, [])
                self.assertIn(b"[REDACTED]", sanitized)
                self.assertNotIn(b"unregistered-token", sanitized)

    def test_split_output_artifacts_and_exit_status(self):
        with tempfile.TemporaryDirectory() as directory:
            # Use a real subprocess to verify both console streams and the resulting files.
            command = [
                sys.executable,
                "-c",
                "import os\nfor fd in (1, 2):\n"
                ' for part in (b"diagnostic: ", b"dummy-", b"secret\\n"):\n'
                "  os.write(fd, part)\nraise SystemExit(17)",
            ]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r}, os.environ, "
                f"guard.secret_forms(os.environ), Path({directory!r})))"
            )
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                cwd=ROOT / "scripts/release",
                env=dict(os.environ, MAVEN_GPG_PASSPHRASE="dummy-secret"),
            )
            self.assertEqual(result.returncode, 17)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, (Path(directory) / (name + ".log")).read_bytes())
                self.assertEqual(console, b"diagnostic: [REDACTED]\n")

    def test_detached_descendant_withholds_incomplete_output(self):
        with tempfile.TemporaryDirectory() as directory:
            command = [
                sys.executable,
                "-c",
                "import os, subprocess\n"
                "subprocess.Popen(['sleep', '4'], start_new_session=True)\n"
                "os.write(1, b\"$'alpha\\\\x09beta\")\n"
                "raise SystemExit(17)",
            ]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                "try:\n"
                f" status = guard.capture({command!r}, os.environ, "
                f"guard.secret_forms(os.environ), Path({directory!r}))\n"
                "except guard.Unsafe:\n raise SystemExit(2)\n"
                "raise SystemExit(status)"
            )
            started = time.monotonic()
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                cwd=ROOT / "scripts/release",
                env=dict(os.environ, MAVEN_GPG_PASSPHRASE="alpha\tbeta"),
                timeout=3,
            )
            self.assertLess(time.monotonic() - started, 3)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, b"")
            self.assertEqual(result.stderr, b"")
            self.assertFalse((Path(directory) / "stdout.log").exists())

    def test_output_failures_preserve_maven_status(self):
        for status in (0, 17):
            for failure in ("directory", "artifact", "stdout", "stderr"):
                with self.subTest(
                    status=status, failure=failure
                ), tempfile.TemporaryDirectory() as directory:
                    logs = Path(directory) / "logs"
                    if failure == "directory":
                        logs.write_text("blocked")
                    elif failure == "artifact":
                        logs.mkdir()
                        (logs / "stdout.log").mkdir()
                    command = [
                        sys.executable, "-c",
                        "import os; os.write(1, b'out\\n'); os.write(2, b'err\\n'); "
                        f"raise SystemExit({status})",
                    ]
                    script = (
                        "import guard, os\nfrom pathlib import Path\n"
                        f"raise SystemExit(guard.capture({command!r}, os.environ, [], "
                        f"Path({str(logs)!r})))"
                    )
                    read_fd, write_fd = os.pipe()
                    os.close(read_fd)
                    try:
                        result = subprocess.run(
                            [sys.executable, "-c", script], cwd=ROOT / "scripts/release",
                            stdout=write_fd if failure == "stdout" else subprocess.PIPE,
                            stderr=write_fd if failure == "stderr" else subprocess.PIPE,
                        )
                    finally:
                        os.close(write_fd)
                    self.assertEqual(result.returncode, status)
                    if failure != "stdout":
                        self.assertEqual(result.stdout, b"out\n")
                    if failure != "stderr":
                        self.assertIn(b"err\n", result.stderr)
                        self.assertIn(b"values withheld", result.stderr)
                    if failure in ("stdout", "stderr"):
                        self.assertEqual((logs / "stdout.log").read_bytes(), b"out\n")
                    if failure != "directory":
                        self.assertEqual((logs / "stderr.log").read_bytes(), b"err\n")

    def test_capture_io_failures_preserve_only_completed_maven_status(self):
        for operation in ("select", "read"):
            for status in (0, 17, -signal.SIGTERM, None):
                with self.subTest(operation=operation, status=status), \
                        tempfile.TemporaryDirectory() as directory:
                    logs = Path(directory) / "logs"
                    code = ("import os, time; os.write(1, b'private incomplete output'); "
                            "os.write(2, b'private error output'); ")
                    if status is None:
                        code += "time.sleep(30)"
                    elif status < 0:
                        code += f"os.kill(os.getpid(), {-status})"
                    else:
                        code += f"raise SystemExit({status})"
                    process = subprocess.Popen(
                        [sys.executable, "-c", code], stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, start_new_session=True,
                    )
                    try:
                        if status is not None:
                            self.assertEqual(process.wait(timeout=5), status)
                        selector = guard.selectors.DefaultSelector()
                        original = selector.select if operation == "select" else os.read
                        calls = 0

                        def fail_after_progress(*args):
                            nonlocal calls
                            calls += 1
                            if calls == 2:
                                raise OSError("private exception value")
                            return original(*args)

                        failure = patch.object(
                            selector, "select", side_effect=fail_after_progress
                        ) if operation == "select" else patch.object(
                            guard.os, "read", side_effect=fail_after_progress
                        )
                        diagnostics = io.StringIO()
                        with patch.object(guard.subprocess, "Popen", return_value=process), \
                                patch.object(guard.selectors, "DefaultSelector", return_value=selector), \
                                patch.object(sys, "argv", ["guard.py", "run", "mvn", "verify"]), \
                                patch.dict(os.environ, dict(clean_guard_env(), RELEASE_LOG_DIR=str(logs)), clear=True), \
                                patch.object(sys, "stdout", io.StringIO()) as stdout, \
                                patch.object(sys, "stderr", diagnostics), \
                                patch.object(guard.os, "write") as write, failure:
                            result = guard.main()
                        expected = 2 if status is None else status if status >= 0 else 128 - status
                        self.assertEqual(result, expected)
                        self.assertEqual(calls, 2)
                        self.assertEqual(stdout.getvalue(), "")
                        diagnostic = diagnostics.getvalue().encode() + b"".join(
                            call.args[1] for call in write.call_args_list
                        )
                        self.assertIn(b"values withheld", diagnostic)
                        self.assertNotIn(b"private", diagnostic)
                        self.assertFalse(logs.exists())
                        if status is None:
                            self.assertEqual(process.returncode, -signal.SIGKILL)
                    finally:
                        if process.poll() is None:
                            process.kill()
                            process.wait()
                        process.stdout.close()
                        process.stderr.close()

    def test_sanitization_and_memory_failures_write_no_logs(self):
        for failure in (
            patch.object(guard, "redact", side_effect=ValueError("secret")),
            patch.object(guard, "LIMIT", 1),
        ):
            with tempfile.TemporaryDirectory() as directory, failure:
                logs = Path(directory) / "logs"
                with self.assertRaises((ValueError, guard.Unsafe)):
                    guard.capture([sys.executable, "-c", 'print("secret")'], os.environ, [], logs)
                self.assertFalse(logs.exists())


if __name__ == "__main__":
    unittest.main()
