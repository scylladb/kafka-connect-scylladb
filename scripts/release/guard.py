#!/usr/bin/env python3
"""Check release defaults and capture Maven output without writing raw credentials.

Output is buffered until Maven exits (64 MiB maximum), then sanitized for both the
console and log artifacts. Child Maven processes run normally; they are not intercepted.
"""

import base64
from bisect import bisect_right
import heapq
import html
import json
import os
from pathlib import Path
import re
import selectors
import shlex
import signal
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET

LIMIT = 64 * 1024 * 1024
EXIT_DRAIN_GRACE = 1.0
DECODE_DEPTH = 8
DECODE_STATES = 128
DECODE_BYTES = 4 * LIMIT
DECODE_TEMP_BYTES = 34 * LIMIT
DECODE_WORK = 32 * LIMIT
DECODE_RECORDS = 500000
ENV_OPTIONS = (
    "MAVEN_ARGS",
    "MAVEN_OPTS",
    "JAVA_TOOL_OPTIONS",
    "JDK_JAVA_OPTIONS",
    "_JAVA_OPTIONS",
    "MAVEN_DEBUG_OPTS",
    "MAVEN_CONFIG",
)
CHILD_OPTIONS = r"arguments|prepareVerifyArgs|goals|preparationGoals|completionGoals"


class Unsafe(ValueError):
    pass


def check_text(text, source=None, maven_options=True, maven_arguments=(), jvm_options=False):
    if any(ord(character) < 32 and character not in "\t\n\r" for character in text):
        raise Unsafe("control bytes in Maven options are not allowed")
    if jvm_options and any(character in text for character in "*?["):
        raise Unsafe("shell patterns in JVM options are not allowed")
    if jvm_options and "-xx:vmoptionsfile" in text.lower().translate(
        str.maketrans("", "", "\"'")
    ):
        raise Unsafe("unverified JVM option files are not allowed")
    # Maven accepts an equals sign after -D/--D; treat it as the ordinary
    # property form before every safety check.
    text = re.sub(r"(?<![\w-])--?D=", "-D", text, flags=re.I)
    text = re.sub(r"(?i)(?<![\w-])(--?define=)\s+", r"\1", text)
    # Maven also trims whitespace around property names. Canonicalize before
    # inspecting either full source text or parsed argument values.
    text = re.sub(
        r"(?i)(?<![\w-])((?:--?D\s*|--?define[=\s]+)[\w.-]+)\s*=",
        r"\1=", text,
    )
    # A quote pair may enclose an entire child-release property. Keep the
    # parsed value visible to the child-option checks below.
    quoted_child = rf"(--?D|--?define=)([\"'])((?:{CHILD_OPTIONS})=[^\"'\s]*)\2"
    text = re.sub(
        rf"(?<![\w-]){quoted_child}", lambda match: match[1] + match[3], text,
        flags=re.I,
    )
    maven_arguments = tuple(
        re.sub(rf"^{quoted_child}$", lambda match: match[1] + match[3], argument,
               flags=re.I)
        for argument in maven_arguments
    )
    if re.search(
        r"(?i)(?<![\w-])(?:--?D\s*|--?define[=\s]+)[\"']\s*[\w.-]+\s*=", text
    ):
        raise Unsafe("quoted Maven property names are not allowed")
    if re.search(
        r"(?i)(?:--?D\s*|--?define(?:=|\s+))[\"']?gpg\.bestPractices[\"']?\s*=\s*[^\s]*[\"']",
        text,
    ):
        raise Unsafe("GPG best practices must not be disabled")
    maven_arguments = tuple(
        re.sub(
            r"(?i)^((?:--?D\s*|--?define[=\s]+)[\w.-]+)\s*=", r"\1=",
            re.sub(
                r"(?i)^--?define=\s+", lambda match: match[0].rstrip(),
                re.sub(r"^--?D=", "-D", argument, flags=re.I),
            ),
        )
        for argument in maven_arguments
    )
    maven_arguments = tuple(
        re.sub(r"^([\w.-]+)\s*=", r"\1=", argument)
        if index and maven_arguments[index - 1] in ("-D", "--D", "-define", "--define")
        else argument
        for index, argument in enumerate(maven_arguments)
    )
    def check_autopublish(argument):
        setting = re.fullmatch(
            r"(?:--?D\s*|--?define(?:=|\s+))release\.autopublish=(.*)",
            argument, re.I | re.S,
        )
        if setting and setting[1] not in ("true", "false", "${release.autopublish}"):
            raise Unsafe("release.autopublish must be a Boolean")

    try:
        arguments = [argument.strip().strip("\"'") for argument in maven_arguments]
        jvm_arguments = shlex.split(text) if jvm_options else []
    except ValueError as error:
        raise Unsafe("malformed release options are not allowed") from error
    def canonical_option(argument):
        # Maven removes quotes inside an option token before interpreting its flag.
        return argument.translate(str.maketrans("", "", "\"'"))

    def check_pom_option(argument):
        argument = canonical_option(argument)
        if (argument.startswith("-f") and argument not in ("-fae", "-ff", "-fn")) or (
            argument.startswith("--f")
            and argument not in ("--fail-at-end", "--fail-fast", "--fail-never")
        ):
            raise Unsafe("alternate Maven POMs are not allowed")
        if (argument.startswith(("-s", "-gs", "-global-settings"))
            and not argument.startswith("--")) or (
            argument in ("--s", "--gs", "--settings", "--global-settings")
            or argument.startswith(("--s=", "--gs=", "--settings=", "--global-settings="))
        ):
            raise Unsafe("alternate Maven settings are not allowed")
        # Maven splits a short flag followed by another hyphenated option.
        # Inspecting only the start of the original token would miss the latter.
        if re.match(r"^-[A-Za-z]=?--?[A-Za-z]", argument):
            raise Unsafe("combined Maven options are not allowed")
        if (re.match(r"^--?[A-Za-z][\w-]*=--?[A-Za-z]", argument)
            and not re.match(r"^--?D[\w.-]*=|^--?define=", argument, re.I)):
            raise Unsafe("combined Maven options are not allowed")

    def check_release_arguments(value):
        if "${" in value:
            raise Unsafe("release arguments must not contain unverified properties")
        if "\\" in value:
            raise Unsafe("backslash escapes in release arguments are not allowed")
        try:
            child_arguments = shlex.split(value)
        except ValueError as error:
            raise Unsafe("malformed release options are not allowed") from error
        for child_argument in child_arguments:
            if canonical_option(child_argument).startswith(("-X", "--X", "-debug", "--debug")):
                raise Unsafe("Maven debug options are not allowed")
            check_pom_option(child_argument)

    for argument in arguments:
        if canonical_option(argument).startswith(("-X", "--X", "-debug", "--debug")):
            raise Unsafe("Maven debug options are not allowed")
        check_pom_option(argument)
    # Release-plugin properties are passed on to child Maven invocations.
    option_arguments = arguments + jvm_arguments
    for index, argument in enumerate(option_arguments):
        if index and option_arguments[index - 1] in ("-D", "--D", "-define", "--define"):
            argument = option_arguments[index - 1] + " " + argument
        check_autopublish(argument)
        setting = re.fullmatch(
            rf"(?:--?D\s*|--?define(?:=|\s+))(?:{CHILD_OPTIONS})=(.*)",
            argument, re.I | re.S,
        )
        if setting:
            check_release_arguments(setting[1])
    if not option_arguments:
        for setting in re.finditer(
            rf"(?:--?D\s*|--?define(?:=|\s+))(?:{CHILD_OPTIONS})=([^\r\n]*)",
            text, re.I,
        ):
            check_release_arguments(setting[1])
    if any(argument.startswith("@") for argument in arguments + jvm_arguments):
        raise Unsafe("unverified argument files are not allowed")
    if jvm_options and re.search(r"-Dgpg\.[^\s]*bestPractices[^\s]*[\"']", text, re.I):
        raise Unsafe("GPG best practices must not be disabled")
    for argument in arguments + jvm_arguments:
        check_autopublish(argument)
    for index, argument in enumerate(maven_arguments):
        if index and maven_arguments[index - 1] in ("-D", "--D", "-define", "--define"):
            argument = maven_arguments[index - 1] + " " + argument
        check_autopublish(argument)
        setting = re.fullmatch(
            r"(?:--?D\s*|--?define(?:=|\s+))gpg\.bestPractices=(.*)", argument, re.I | re.S
        )
        if setting and setting[1].lower() != "true":
            raise Unsafe("GPG best practices must not be disabled")
    # Preserve whitespace inside quoted GPG values when checking the full boolean.
    gpg_text = re.sub(
        r"([\"'])((?:\\.|(?!\1)[\s\S])*)\1",
        lambda match: re.sub(r"\s", "\0", match[2]), text,
    ).translate(str.maketrans("", "", "\"'"))
    if re.search(r"(?:--?D\s*|--?define(?:=|\s+))gpg\.bestPractices=(?!true(?:\s|$))",
                 gpg_text, re.I) or (jvm_options and re.search(
                     r"(?:^|\s)-Dgpg\.bestPractices(?:\s|$)", gpg_text, re.I)):
        raise Unsafe("GPG best practices must not be disabled")
    # Tokenizers may remove quoted whitespace in the property name. The
    # value check above masks whitespace inside quoted values, so check the
    # unquoted spelling separately as well.
    unquoted = text.translate(str.maketrans("", "", "\"'"))
    if re.search(
        r"(?:--?D\s*|--?define(?:=|\s+))gpg\.bestPractices\s*=\s*(?!true(?:\s|$))",
        unquoted, re.I,
    ):
        raise Unsafe("GPG best practices must not be disabled")
    # Java and Maven option parsing remove quotes from property names and values.
    text = unquoted
    if re.search(r"(?:--?D\s*|--?define(?:=|\s+))pomFileName\b", text, re.I):
        raise Unsafe("alternate Maven POMs are not allowed")
    if maven_options and re.search(
        r"(?<![\w-])(?:--?s|--?gs|--settings|--global-settings)(?=\s|=|$)", text
    ):
        raise Unsafe("alternate Maven settings are not allowed")
    # Catch accidental debug flags and credential properties, including release arguments.
    match = re.search(
        r"(?:^|[\s=\"'])(?:--?X|--?debug)\b"
        r"|(?:--?D\s*|--?define(?:=|\s+))(?:arguments|prepareVerifyArgs)="
        r"(?:-X|[^\r\n]*?[=\s\"']-X)"
        r"|(?:--?D\s*|--?define(?:=|\s+))javax\.net\.debug\b"
        r"|simpleLogger\.logFile\b"
        r"|simpleLogger[^\s<>]*=(?:debug|trace)\b"
        r"|simpleLogger[^\s<>]*=\$\{"
        r"|(?:--?D\s*|--?define(?:=|\s+))"
        r"(?:user\.(?:dir|home)|maven\.(?:conf|multiModuleProjectDirectory)"
        r"|sun\.(?:stdout|stderr)\.encoding|(?:stdout|stderr|file|native)\.encoding)\b"
        r"|(?:--?D\s*|--?define(?:=|\s+))[\w.-]*"
        r"(?:passphrase|password|token|secret|authorization|credential)[\w.-]*=",
        text,
        re.I,
    )
    if maven_options and not match:
        match = re.search(
            r"(?:^|[\s=\"'])(?:--log-file(?:[=\s]|$)|-l(?!lr(?:[\s\"']|$))|--l)", text
        )
    if match:
        if source is not None:
            start = match.start() + len(match[0]) - len(match[0].lstrip())
            line = text.count("\n", 0, start) + 1
            print(f"Unsafe release options at {source}:{line}; values withheld.", file=sys.stderr)
        raise Unsafe("debug logging or credential arguments are not allowed")


def check_policy(root):
    check_ancestor_maven_config(root)
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    try:
        pom = ET.parse(root / "pom.xml").getroot()
    except (OSError, ET.ParseError) as error:
        raise Unsafe("target POM is missing or malformed") from error
    if pom.find("m:parent", ns) is not None:
        raise Unsafe("target POM inherits unverified parent release configuration")
    if pom.findtext("m:properties/m:gpg.bestPractices", namespaces=ns) != "true":
        raise Unsafe("enable GPG best practices before releasing this target")
    root_plugins = pom.findall("m:build/m:plugins/m:plugin", ns)
    supported_gpg = False
    supported_central = False
    release_configuration = set()
    property_values = set(pom.findall(".//m:properties/*", ns))
    for plugin in pom.findall(".//m:plugin", ns):
        if plugin.findtext("m:artifactId", namespaces=ns) == "maven-release-plugin":
            release_configuration.update(plugin.findall(".//m:configuration//*", ns))
        if plugin.findtext("m:artifactId", namespaces=ns) == "maven-gpg-plugin":
            version = plugin.findtext("m:version", namespaces=ns)
            if version is not None and (
                not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version)
                or tuple(map(int, version.split("."))) < (3, 2, 0)
            ):
                raise Unsafe("update the GPG plugin before releasing this target")
            if plugin in root_plugins and version is not None:
                supported_gpg = True
            for setting in plugin.findall(".//m:bestPractices", ns):
                if (setting.text or "").strip().lower() != "true":
                    raise Unsafe("GPG best practices must not be disabled")
        if plugin.findtext("m:artifactId", namespaces=ns) == "central-publishing-maven-plugin":
            version = plugin.findtext("m:version", namespaces=ns)
            if version is not None and (
                not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version)
                or tuple(map(int, version.split("."))) < (0, 11, 0)
            ):
                raise Unsafe("update Central publishing before releasing this target")
            if plugin in root_plugins and version is not None:
                supported_central = True
    if not supported_gpg:
        raise Unsafe("pin a GPG plugin supporting best practices before releasing this target")
    if not supported_central:
        raise Unsafe("pin Central publishing before releasing this target")
    for element in pom.iter():
        name = element.tag.rsplit("}", 1)[-1]
        if name == "pomFileName":
            raise Unsafe("alternate Maven POMs are not allowed")
        if (element.tag == "{" + ns["m"] + "}gpg.bestPractices"
                and (element.text or "").strip().lower() != "true"):
            raise Unsafe("GPG best practices must not be disabled")
        if (element.tag == "{" + ns["m"] + "}release.autopublish"
                and (element.text or "").strip() not in ("true", "false")):
            raise Unsafe("release.autopublish must be a Boolean")
        child_arguments = name == "arguments" or (
            name in ("prepareVerifyArgs", "goals", "preparationGoals", "completionGoals")
            and (element in release_configuration or element in property_values)
        )
        if child_arguments and "${" in (element.text or "").replace(
            "-Drelease.autopublish=${release.autopublish}", ""
        ):
            raise Unsafe("release arguments must not contain unverified properties")
        check_text(
            element.text or "",
            maven_options=child_arguments,
            maven_arguments=(element.text or "").split() if child_arguments else (),
        )
    paths = [
        root / "Makefile",
        root / ".github/workflows/release.yml",
        root / ".mvn/maven.config",
        root / ".mvn/jvm.config",
    ]
    for path in paths:
        if path.exists():
            check_text(
                path.read_text(), path.relative_to(root), maven_options=path.parent.name == ".mvn",
                maven_arguments=(
                    path.read_text().splitlines() if path.name == "maven.config" else ()
                ),
                jvm_options=path.name == "jvm.config",
            )
    try:
        workflow = (root / ".github/workflows/release.yml").read_text()
    except (OSError, UnicodeError) as error:
        raise Unsafe("target release workflow is missing or unreadable") from error
    if "RELEASE_GUARD" not in workflow or "if: always()" not in workflow:
        raise Unsafe(
            "release workflow sanity check requires RELEASE_GUARD and if: always() markers"
        )
    makefile = root / "Makefile"
    if makefile.exists():
        for line in makefile.read_text().splitlines():
            if re.search(r"\$\(MVNCMD\).*\brelease:", line) and "$(RELEASE_GUARD)" not in line:
                raise Unsafe("release target must use sanitized log capture")


def secret_forms(env):
    values = {
        v
        for k, v in env.items()
        if v and re.search(
            r"PASSWORD|PASSPHRASE|TOKEN|SECRET|PRIVATE_KEY|AUTHORIZATION|CREDENTIAL", k, re.I
        )
    }
    user, password = env.get("SONATYPE_TOKEN_USERNAME"), env.get("SONATYPE_TOKEN_PASSWORD")
    if user and password:
        values.add(user + ":" + password)
    # Include multiline components, escaped newlines, URL and both Base64 alphabets.
    values |= {line for value in values for line in value.splitlines() if line}
    values |= {value.replace("\n", "\\n").replace("\r", "\\r") for value in values}
    forms = set(values)
    for value in values:
        raw = value.encode()
        json_forms = (json.dumps(value), json.dumps(value, ensure_ascii=False))
        json_forms += tuple(encoded.replace("/", "\\/") for encoded in json_forms)
        forms.update(
            (
                base64.b64encode(raw).decode(),
                base64.urlsafe_b64encode(raw).decode(),
                *(form for encoded in json_forms for form in (encoded, encoded[1:-1])),
                html.escape(value, quote=False),
                html.escape(value, quote=True),
                shlex.quote(value),
                "'" + value.replace("'", "'\\''") + "'",
                re.sub(r"([^\w@%+=:,./-])", r"\\\1", value),
                urllib.parse.quote(value, safe=""),
                urllib.parse.quote(value),
                urllib.parse.quote_plus(value, safe=""),
                urllib.parse.quote_plus(value, safe="*").replace("~", "%7E"),
            )
        )
        for encoded in json_forms:
            for character in "<>&='":
                encoded = encoded.replace(character, "\\u%04x" % ord(character))
            forms.update((encoded, encoded[1:-1]))
        ansi_escapes = {"\a": "\\a", "\b": "\\b", "\x1b": "\\e", "\f": "\\f",
                        "\n": "\\n", "\r": "\\r", "\t": "\\t", "\v": "\\v",
                        "\\": "\\\\", "'": "\\'"}
        ansi = "".join(ansi_escapes.get(character, character) for character in value)
        octal = "".join(
            "\\%03o" % byte if byte < 32 or byte >= 127 else ansi_escapes.get(chr(byte), chr(byte))
            for byte in raw
        )
        for escaped in (ansi, octal):
            forms.update(("$'" + escaped + "'", escaped))
        if "\x1b" in value:
            upper_escape = ansi.replace("\\e", "\\E")
            forms.update(("$'" + upper_escape + "'", upper_escape))
    for value in tuple(forms):
        forms.add(urllib.parse.quote(value, safe=""))
        forms.add(urllib.parse.quote(value))
        forms.add(value.rstrip("="))
    return sorted((v.encode() for v in forms if v), key=len, reverse=True)


def redact(data, forms):
    # Match every source against the original bytes so replacements cannot hide secrets.
    def spans(pattern, group, flags=0):
        return (match.span(group) for match in re.finditer(pattern, data, flags))

    def normalize_percent_hex(value):
        # Percent-encoded bytes are case-insensitive; keep ordinary text case-sensitive.
        return re.sub(rb"%[0-9A-Fa-f]{2}", lambda match: match[0].upper(), value)

    normalized_forms = [normalize_percent_hex(value) for value in forms]

    def decode_entities(source):
        view = bytearray()
        replacements = []
        previous = 0
        for entity in re.finditer(
            rb"&(?:#[xX][0-9A-Fa-f]+|#[0-9]+|[A-Za-z][A-Za-z0-9]*);?", source
        ):
            converted = html.unescape(entity[0].decode("ascii")).encode()
            if converted == entity[0]:
                continue
            view.extend(source[previous:entity.start()])
            start = len(view)
            view.extend(converted)
            replacements.append((start, len(view), entity.start(), entity.end()))
            if len(replacements) > 500000:
                raise Unsafe("too many encoded values in release output")
            previous = entity.end()
        view.extend(source[previous:])
        return bytes(view), replacements

    def decode_json_escapes(source):
        view = bytearray()
        replacements = []
        previous = 0
        simple = {
            ord('"'): b'"', ord('\\'): b'\\', ord('/'): b'/',
            ord('b'): b'\b', ord('f'): b'\f', ord('n'): b'\n',
            ord('r'): b'\r', ord('t'): b'\t',
        }
        for escape in re.finditer(rb'\\(?:u[0-9A-Fa-f]{4}|["\\/bfnrt])', source):
            if escape.start() < previous:
                continue
            end = escape.end()
            if escape[0][1] == ord('u'):
                codepoint = int(escape[0][2:], 16)
                if 0xD800 <= codepoint <= 0xDBFF and re.fullmatch(
                    rb"\\u[0-9A-Fa-f]{4}", source[end:end + 6]
                ):
                    low = int(source[end + 2:end + 6], 16)
                    if 0xDC00 <= low <= 0xDFFF:
                        codepoint = 0x10000 + ((codepoint - 0xD800) << 10) + low - 0xDC00
                        end += 6
                if 0xD800 <= codepoint <= 0xDFFF:
                    continue
                converted = chr(codepoint).encode()
            else:
                converted = simple[escape[0][1]]
            view.extend(source[previous:escape.start()])
            start = len(view)
            view.extend(converted)
            replacements.append((start, len(view), escape.start(), end))
            if len(replacements) > 500000:
                raise Unsafe("too many encoded values in release output")
            previous = end
        view.extend(source[previous:])
        return bytes(view), replacements

    def decode_percent_escapes(source):
        view = bytearray()
        replacements = []
        previous = 0
        for escape in re.finditer(rb"%[0-9A-Fa-f]{2}", source):
            view.extend(source[previous:escape.start()])
            start = len(view)
            view.append(int(escape[0][1:], 16))
            replacements.append((start, len(view), escape.start(), escape.end()))
            if len(replacements) > 500000:
                raise Unsafe("too many encoded values in release output")
            previous = escape.end()
        view.extend(source[previous:])
        return bytes(view), replacements

    def decoded_span(replacements, starts, left, right):
        first = bisect_right(starts, left) - 1
        last = bisect_right(starts, right - 1) - 1
        if first >= 0:
            dec_start, dec_end, raw_start, raw_end = replacements[first]
            left = raw_start if left < dec_end else left + raw_end - dec_end
        if last >= 0:
            dec_start, dec_end, raw_start, raw_end = replacements[last]
            right = raw_end if right - 1 < dec_end else right + raw_end - dec_end
        return left, right

    def ansi_c_bytes(value):
        escapes = {ord("a"): 7, ord("b"): 8, ord("e"): 27, ord("E"): 27,
                   ord("f"): 12, ord("n"): 10, ord("r"): 13, ord("t"): 9,
                   ord("v"): 11}
        result = bytearray()
        index = 0
        while index < len(value):
            character = value[index]
            if character != 92 or index + 1 == len(value):
                result.append(character)
                index += 1
                continue
            index += 1
            character = value[index]
            if character in escapes:
                result.append(escapes[character])
                index += 1
            elif 48 <= character <= 55:
                end = index + 1
                while end < min(index + 3, len(value)) and 48 <= value[end] <= 55:
                    end += 1
                result.append(int(value[index:end], 8) & 255)
                index = end
            elif character in (ord("x"), ord("u"), ord("U")):
                limit = 2 if character == ord("x") else 4 if character == ord("u") else 8
                end = index + 1
                while end < min(index + 1 + limit, len(value)) and value[end] in b"0123456789abcdefABCDEF":
                    end += 1
                if end == index + 1:
                    result.extend((92, character))
                else:
                    number = int(value[index + 1:end], 16)
                    try:
                        result.extend(chr(number).encode() if character != ord("x") else bytes((number,)))
                    except (ValueError, UnicodeEncodeError):
                        result.extend(value[index - 1:end])
                index = end
            elif character == ord("c") and index + 1 < len(value):
                result.append(127 if value[index + 1] == ord("?") else value[index + 1] & 31)
                index += 3 if value[index + 1:index + 3] == b"\\\\" else 2
            elif character in (ord("\\"), ord("'"), ord('"'), ord("?")):
                result.append(character)
                index += 1
            else:
                result.extend((92, character))
                index += 1
        return bytes(result)

    # Unknown unquoted values have no reliable end boundary: withhold the
    # remaining stream, including newline continuations. Only a closing quote
    # followed by whitespace/EOF is a supported boundary. Adjacent shell
    # fragments fall back to masking the stream, regardless of fragment count.
    property_patterns = (
        (
            rb"(?i)([\"'])((?<![\w.-])(?:--?D[=\s]*|--?define[=\s]+)[\w.-]*"
            rb"(?:passphrase|password|username|token|secret|authorization|credential)[\w.-]*\s*=)"
            rb"((?:\\[\s\S]|(?!\1)[^\\])*)(?:\1(?=\s|$)|$)", 3,
        ),
        (
            rb"(?i)((?<![\w.-])(?:--?D[=\s]*|--?define[=\s]+)[\w.-]*"
            rb"(?:passphrase|password|username|token|secret|authorization|credential)[\w.-]*\s*=)"
            rb"\s*(\"(?:\\[\s\S]|[^\"\\])*(?:\"(?=\s|$)|$)|'(?:\\[\s\S]|[^'\\])*(?:'(?=\s|$)|$)|(?!\\[\"'])[\s\S]+)", 2,
        ),
    )
    authorization_pattern = (
        rb"(?im)((?:proxy-)?authorization(?:\\?[\"'])?[ \t]*[:=][ \t]*)"
        rb"([^\r\n]*(?:(?:\r\n|\n|\r)[ \t]+[^\r\n]*)*)"
    )
    matches = [spans(pattern, group) for pattern, group in property_patterns]
    matches.append(spans(authorization_pattern, 2))
    for value in forms:
        # Lookahead includes overlapping occurrences; percent escapes ignore case.
        matches.append(spans(
            b"(?=(" + re.escape(value) + b"))", 1,
            re.I if b"%" in value or b"\\u" in value else 0,
        ))
    # URL encoders may mix escaped bytes and literal plus signs. Decode only
    # percent triplets; plus signs are literal outside form-encoded values.
    if b"%" in data:
        percent_view, percent_replacements = decode_percent_escapes(data)
        if percent_replacements:
            percent_starts = [item[0] for item in percent_replacements]

            def percent_spans(pattern, group):
                for match in re.finditer(pattern, percent_view):
                    if match.start(group) < match.end(group):
                        yield decoded_span(
                            percent_replacements, percent_starts, *match.span(group)
                        )

            matches.extend(
                percent_spans(b"(?=(" + re.escape(value) + b"))", 1)
                for value in forms
            )
            matches.extend(
                percent_spans(pattern, group)
                for pattern, group in property_patterns
            )
            matches.append(percent_spans(authorization_pattern, 2))
    # Decode HTML entities for matching, retaining byte offsets so the original
    # encoded text can be replaced without emitting a recoverable credential.
    if b"&" in data:
        html_decoded = bytearray()
        html_replacements = []
        previous = 0
        for entity in re.finditer(
            rb"&(?:#[xX][0-9A-Fa-f]+|#[0-9]+|[A-Za-z][A-Za-z0-9]*);?", data
        ):
            converted = html.unescape(entity[0].decode("ascii")).encode()
            if converted == entity[0]:
                continue
            html_decoded.extend(data[previous:entity.start()])
            start = len(html_decoded)
            html_decoded.extend(converted)
            html_replacements.append((start, len(html_decoded), entity.start(), entity.end()))
            if len(html_replacements) > 500000:
                raise Unsafe("too many encoded values in release output")
            previous = entity.end()
        if html_replacements:
            html_decoded.extend(data[previous:])
            html_starts = [item[0] for item in html_replacements]
            normalized_html_decoded = normalize_percent_hex(bytes(html_decoded))

            def raw_start(position):
                index = bisect_right(html_starts, position) - 1
                if index < 0:
                    return position
                dec_start, dec_end, original_start, original_end = html_replacements[index]
                return original_start if position < dec_end else position + original_end - dec_end

            def raw_end(position):
                index = bisect_right(html_starts, position - 1) - 1
                if index < 0:
                    return position
                dec_start, dec_end, original_start, original_end = html_replacements[index]
                return original_end if position - 1 < dec_end else position + original_end - dec_end

            def entity_spans(value):
                pattern = b"(?=(" + re.escape(value) + b"))"
                for match in re.finditer(pattern, normalized_html_decoded):
                    left, right = match.span(1)
                    yield raw_start(left), raw_end(right)

            matches.extend(entity_spans(value) for value in normalized_forms)

            def html_pattern_spans(pattern, group):
                for match in re.finditer(pattern, html_decoded):
                    left, right = match.span(group)
                    if left < right:
                        yield raw_start(left), raw_end(right)

            matches.extend(
                html_pattern_spans(pattern, group)
                for pattern, group in property_patterns
            )
            matches.append(html_pattern_spans(
                authorization_pattern,
                2,
            ))
    # JSON serializers can escape only some characters in a credential.
    # Decode standard JSON escapes together so mixed forms match, while
    # replacing the exact raw byte span.
    if b"\\" in data:
        decoded = bytearray()
        json_replacements = []
        previous = 0
        simple_escapes = {
            ord('"'): b'"', ord('\\'): b'\\', ord('/'): b'/',
            ord('b'): b'\b', ord('f'): b'\f', ord('n'): b'\n',
            ord('r'): b'\r', ord('t'): b'\t',
        }
        for escape in re.finditer(rb'\\(?:u[0-9A-Fa-f]{4}|["\\/bfnrt])', data):
            if escape.start() < previous:
                continue
            end = escape.end()
            if escape[0][1] == ord('u'):
                codepoint = int(escape[0][2:], 16)
                if 0xD800 <= codepoint <= 0xDBFF and re.fullmatch(
                    rb"\\u[0-9A-Fa-f]{4}", data[end:end + 6]
                ):
                    low = int(data[end + 2:end + 6], 16)
                    if 0xDC00 <= low <= 0xDFFF:
                        codepoint = 0x10000 + ((codepoint - 0xD800) << 10) + low - 0xDC00
                        end += 6
                if 0xD800 <= codepoint <= 0xDFFF:
                    continue
                converted = chr(codepoint).encode()
            else:
                converted = simple_escapes[escape[0][1]]
            decoded.extend(data[previous:escape.start()])
            start = len(decoded)
            decoded.extend(converted)
            json_replacements.append((start, len(decoded), escape.start(), end))
            if len(json_replacements) > 500000:
                raise Unsafe("too many encoded values in release output")
            previous = end
        if json_replacements:
            decoded.extend(data[previous:])
            json_starts = [item[0] for item in json_replacements]

            def raw_json_span(left, right):
                first = bisect_right(json_starts, left) - 1
                last = bisect_right(json_starts, right - 1) - 1
                if first >= 0:
                    dec_start, dec_end, raw_start, raw_end = json_replacements[first]
                    left = raw_start if left < dec_end else left + raw_end - dec_end
                if last >= 0:
                    dec_start, dec_end, raw_start, raw_end = json_replacements[last]
                    right = raw_end if right - 1 < dec_end else right + raw_end - dec_end
                return left, right

            normalized_decoded = normalize_percent_hex(bytes(decoded))

            def json_spans(value):
                for match in re.finditer(
                    b"(?=(" + re.escape(value) + b"))", normalized_decoded
                ):
                    yield raw_json_span(*match.span(1))

            matches.extend(json_spans(value) for value in normalized_forms)

            def json_property_spans(pattern, group):
                for match in re.finditer(pattern, decoded):
                    yield raw_json_span(*match.span(group))

            matches.extend(
                json_property_spans(pattern, group)
                for pattern, group in property_patterns
            )

            def json_authorization_spans():
                for match in re.finditer(
                    authorization_pattern,
                    decoded,
                ):
                    if match.start(2) < match.end(2):
                        yield raw_json_span(*match.span(2))

            matches.append(json_authorization_spans())
    work = 0
    # Traverse changing HTML/JSON/percent paths, including repeated formats.
    # Standalone paths above stay independent. Sparse parent maps retain offsets.
    if b"\\" in data or b"&" in data or b"%" in data:
        states = [(data, (), 0)]
        retained_bytes = records = 0
        decoders = (
            (b"&", decode_entities,
             rb"&(?:#[xX][0-9A-Fa-f]+|#[0-9]+|[A-Za-z][A-Za-z0-9]*);?"),
            (b"\\", decode_json_escapes, rb'\\(?:u[0-9A-Fa-f]{4}|["\\/bfnrt])'),
            (b"%", decode_percent_escapes, rb"%[0-9A-Fa-f]{2}"),
        )
        for source, parents, depth in states:
            for marker, decoder, pattern in decoders:
                work += len(source)  # Charge even unsuccessful marker scans.
                if work > DECODE_WORK:
                    raise Unsafe("release output decoding work budget exceeded")
                if marker not in source:
                    continue
                size = len(source)
                work += 6 * size  # Candidate/decode scans and up to 2*n normalization.
                if work > DECODE_WORK:
                    raise Unsafe("release output decoding work budget exceeded")
                candidates = 0
                for _ in re.finditer(pattern, source):
                    candidates += 1
                    if records + candidates > DECODE_RECORDS:
                        raise Unsafe("release output decoding mapping budget exceeded")
                records += candidates  # Conservative, including unknown HTML entities.
                if not candidates:
                    continue
                # Preflight unchanged decoders: HTML can expand to at most 2*n.
                # Reserve decoded/normalized views before invoking the decoder.
                # Temporary cap covers bytearray/bytes/slices, HTML Unicode
                # intermediates and up to 500k substitution fragments (128 each).
                # Mapping tuples/integers/starts have the separate record cap.
                if retained_bytes + 4 * size > DECODE_BYTES:
                    raise Unsafe("release output decoding byte budget exceeded")
                if 32 * size + 128 * DECODE_RECORDS > DECODE_TEMP_BYTES:
                    raise Unsafe("release output decoding temporary budget exceeded")
                final_view, replacements = decoder(source)
                if not replacements:
                    del final_view, replacements
                    continue
                if depth >= DECODE_DEPTH or len(states) >= DECODE_STATES:
                    raise Unsafe("release output decoding depth or state budget exceeded")
                retained_bytes += 2 * len(final_view)
                starts = [item[0] for item in replacements]
                chain = ((replacements, starts),) + parents
                states.append((final_view, chain, depth + 1))
                # Bound re.sub's temporary fragment list before normalization.
                for count, _ in enumerate(re.finditer(rb"%[0-9A-Fa-f]{2}", final_view), 1):
                    if count > DECODE_RECORDS:
                        raise Unsafe("release output decoding normalization budget exceeded")
                normalized_view = normalize_percent_hex(final_view)

                def composed_span(left, right, chain=chain):
                    for replacements, starts in chain:
                        left, right = decoded_span(replacements, starts, left, right)
                    return left, right

                def composed_form_spans(value, normalized_view=normalized_view,
                                        composed_span=composed_span):
                    pattern = b"(?=(" + re.escape(value) + b"))"
                    for match in re.finditer(pattern, normalized_view):
                        yield composed_span(*match.span(1))

                matches.extend(composed_form_spans(value) for value in normalized_forms)

                def composed_ansi_spans(final_view=final_view,
                                        composed_span=composed_span):
                    for left, right in ansi_c_spans(final_view, escaped_only=True):
                        yield composed_span(left, right)

                matches.append(composed_ansi_spans())

                def composed_pattern_spans(pattern, group, final_view=final_view,
                                           composed_span=composed_span):
                    for match in re.finditer(pattern, final_view):
                        if match.start(group) < match.end(group):
                            yield composed_span(*match.span(group))

                matches.extend(
                    composed_pattern_spans(pattern, group)
                    for pattern, group in property_patterns
                )
                matches.append(composed_pattern_spans(
                    authorization_pattern,
                    2,
                ))
    # Keep ANSI-C matches lazy so repeated quoted tokens cannot create one
    # retained iterator per occurrence within the bounded capture buffer.
    def ansi_c_spans(source=data, escaped_only=False):
        nonlocal work
        work += len(source)
        if work > DECODE_WORK:
            raise Unsafe("release output decoding work budget exceeded")
        next_open = 0
        examined = 0
        while (start := source.find(b"$'", next_open)) >= 0:
            # Check every opener. A stray opener can consume the quote of a
            # later valid token, so nonoverlapping regex matches miss it.
            next_open = start + 2
            quote = source.find(b"'", next_open)
            while quote >= 0:
                slash = quote - 1
                while slash >= next_open and source[slash] == 92:
                    slash -= 1
                if (quote - slash - 1) % 2 == 0:
                    break
                quote = source.find(b"'", quote + 1)
            examined += (quote if quote >= 0 else len(source)) - next_open
            if examined > 2 * LIMIT:
                raise Unsafe("too many overlapping encoded values in release output")
            end = quote if quote >= 0 else len(source)
            # Share the composition work budget, including overlapping tokens.
            work += 6 * (end - next_open)
            if work > DECODE_WORK:
                raise Unsafe("release output decoding work budget exceeded")
            token = source[next_open:end]
            decoded = ansi_c_bytes(token)
            # Suppress changed tokens conservatively: later supported decoders
            # can expose credentials without requiring another ANSI byte map.
            # Benign escaped tokens are also masked; unchanged stray openers
            # alone do not trigger suppression.
            if decoded != token:
                yield start, quote + 1 if quote >= 0 else len(source)
                continue
            if escaped_only:
                continue
            percent_decoded, _ = decode_percent_escapes(decoded)
            views = (decoded, percent_decoded)
            normalized_views = tuple(normalize_percent_hex(view) for view in views)
            if (any(value in view for value in normalized_forms
                    for view in normalized_views)
                    or any(re.search(pattern, view) for pattern, _ in property_patterns
                           for view in views)
                    or any(re.search(authorization_pattern, view)
                           for view in views)):
                yield start, quote + 1 if quote >= 0 else len(source)

    matches.append(ansi_c_spans())
    output = bytearray()
    start = end = 0
    for left, right in heapq.merge(*matches):
        if left > end:
            if end > start:
                output.extend(b"[REDACTED]")
            output.extend(data[end:left])
            start = left
        end = max(end, right)
    if end > start:
        output.extend(b"[REDACTED]")
    output.extend(data[end:])
    return bytes(output)


def capture(command, env, forms, logs):
    """Never spool raw output. Limit memory and terminate the process group on failure."""
    process = subprocess.Popen(
        command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True
    )
    buffers = [bytearray(), bytearray()]
    try:
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ, 0)
                selector.register(process.stderr, selectors.EVENT_READ, 1)
                drain_deadline = None
                while selector.get_map():
                    if process.poll() is not None and drain_deadline is None:
                        # A detached descendant may retain the pipes indefinitely.
                        drain_deadline = time.monotonic() + EXIT_DRAIN_GRACE
                    if drain_deadline is not None and time.monotonic() >= drain_deadline:
                        raise Unsafe("release output remains open after Maven exits")
                    timeout = 0.1 if drain_deadline is None else max(
                        0, drain_deadline - time.monotonic()
                    )
                    for key, _ in selector.select(timeout):
                        chunk = os.read(key.fileobj.fileno(), 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                        else:
                            buffers[key.data].extend(chunk)
                            if sum(map(len, buffers)) > LIMIT:
                                raise Unsafe("release output exceeds memory limit")
        except OSError:
            # A capture I/O failure must not turn a completed publication into
            # failure and skip the caller's SCM push. Discard partial output;
            # only a known completed Maven status can be preserved here.
            status = process.poll()
            if status is None:
                raise
            try:
                os.write(2, b"Release output capture failed; values withheld.\n")
            except OSError:
                pass
            return status if status >= 0 else 128 - status
        status = process.wait()
        if any(b"\0" in buffer for buffer in buffers):
            raise Unsafe("unsupported release output encoding")
        # Sanitize both streams completely before opening artifacts or emitting output.
        clean = [redact(bytes(buffer), forms) for buffer in buffers]
        output_failed = False
        for name, data, stream in zip(("stdout", "stderr"), clean, (sys.stdout, sys.stderr)):
            try:
                logs.mkdir(parents=True, exist_ok=True)
                with (logs / (name + ".log")).open("ab") as output:
                    output.write(data)
            except OSError:
                output_failed = True
            try:
                stream.buffer.write(data)
                stream.flush()
            except OSError:
                output_failed = True
                # Prevent a failed shutdown flush from replacing Maven's exit status.
                try:
                    with open(os.devnull, "wb") as sink:
                        os.dup2(sink.fileno(), stream.fileno())
                except OSError:
                    pass
        if output_failed:
            try:
                os.write(
                    2, b"Sanitized release output could not be fully written; values withheld.\n"
                )
            except OSError:
                pass
        return status if status >= 0 else 128 - status
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        raise
    finally:
        process.stdout.close()
        process.stderr.close()


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


def check_environment_options():
    for name in ENV_OPTIONS:
        value = os.environ.get(name, "")
        if name in ("MAVEN_ARGS", "MAVEN_CONFIG") and any(
            character in value for character in "*?["
        ):
            raise Unsafe("shell patterns in Maven options are not allowed")
        check_text(
            value,
            maven_arguments=value.split() if name in ("MAVEN_ARGS", "MAVEN_CONFIG") else (),
            jvm_options=name not in ("MAVEN_ARGS", "MAVEN_CONFIG"),
        )


def check_ancestor_maven_config(root):
    # Maven searches parent directories when this checkout has no .mvn.
    # Those files are outside the selected release target's policy check.
    if (root / ".mvn").is_dir():
        return
    for parent in root.parents:
        if any((parent / ".mvn" / name).exists()
               for name in ("maven.config", "jvm.config")):
            raise Unsafe("ancestor Maven configuration is not allowed")


def main():
    try:
        if os.environ.get("MAVEN_BASEDIR"):
            raise Unsafe("MAVEN_BASEDIR can select unchecked Maven configuration")
        if sys.argv[1:] == ["check"]:
            try:
                check_environment_options()
                check_policy(Path.cwd())
            except Unsafe as error:
                print(
                    "Selected release target fails credential safety checks. "
                    "Release only supported targets with the release guard and "
                    f"safe GPG/Central configuration: {error}; values withheld.",
                    file=sys.stderr,
                )
                return 2
            print("Release credential safety checks passed.")
            return 0
        if sys.argv[1:3] != ["run", "mvn"]:
            raise Unsafe("expected check or run mvn ...")
        if any(arg.startswith("-X") for arg in sys.argv[3:]):
            raise Unsafe("Maven debug options are not allowed")
        check_text(" ".join(sys.argv[3:]), maven_arguments=sys.argv[3:])
        check_environment_options()
        check_ancestor_maven_config(Path.cwd())
        for name in ("maven.config", "jvm.config"):
            path = Path(".mvn") / name
            if path.exists():
                check_text(
                    path.read_text(),
                    maven_arguments=path.read_text().splitlines() if name == "maven.config" else (),
                    jvm_options=name == "jvm.config",
                )
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, interrupted)
        maven_env = os.environ.copy()
        maven_env["MAVEN_SKIP_RC"] = "1"
        return capture(
            sys.argv[2:],
            maven_env,
            secret_forms(os.environ),
            Path(os.environ.get("RELEASE_LOG_DIR", "/tmp/release-logs")),
        )
    except Unsafe as error:
        print(f"Release safety check failed: {error}; values withheld.", file=sys.stderr)
        return 2
    except Exception:
        # Exception values can contain credentials. Do not echo them.
        print(
            "Release safety check or sanitized log capture failed; values withheld.",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
