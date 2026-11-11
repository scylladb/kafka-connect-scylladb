#!/usr/bin/env python3
"""Read prepared Maven identity and sign/verify the GitHub distribution assets."""

import json
import os
from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET

import signing


def check_tokens():
    for name in ("SONATYPE_TOKEN_USERNAME", "SONATYPE_TOKEN_PASSWORD"):
        value = os.environ.get(name, "").strip()
        if not value or value == "PLACEHOLDER_NOT_CONFIGURED":
            raise signing.SigningError("Configure real Sonatype tokens before preparing or uploading a release.")


def identity():
    # Maven's properties writer escapes the colon in project coordinates.
    properties = {}
    metadata = Path("release.properties")
    if not metadata.exists():
        metadata = signing.PREPARED_METADATA
    for line in metadata.read_text().splitlines():
        if not line or line.startswith(("#", "!")):
            continue
        key, separator, value = line.partition("=")
        key = key.replace("\\:", ":")
        if separator:
            if key in properties:
                raise signing.SigningError("Duplicate prepared Maven metadata.")
            properties[key] = value
    tag = properties.get("scm.tag", "")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", tag):
        raise signing.SigningError("Expected a prepared X.Y.Z release tag.")
    version = tag
    if properties.get("project.rel.com.scylladb:kafka-connect-scylladb") != version:
        raise signing.SigningError("Prepared Maven version and release tag disagree.")
    pom = ET.fromstring(signing.run(["git", "show", "refs/tags/" + tag + ":pom.xml"]))
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    if (pom.findtext("m:groupId", namespaces=ns) != "com.scylladb"
            or pom.findtext("m:artifactId", namespaces=ns) != "kafka-connect-scylladb"
            or pom.findtext("m:version", namespaces=ns) != version):
        raise signing.SigningError("Prepared tag does not contain the expected Maven release identity.")
    return tag, version


def artifacts():
    _, version = identity()
    target = Path("target/checkout/target")
    return [target / f"kafka-connect-scylladb-{version}.jar",
            target / "components/packages" / f"scylladb-kafka-connect-scylladb-{version}.zip"]


def verify_remote_tag(tag):
    """Require local and origin refs to retain the identity verified at preparation."""
    metadata = signing.PREPARED_METADATA.read_text()
    expected = {}
    for key in ("tag", "tagObject", "commit"):
        values = re.findall(r"^scylladb\.prepared\." + key + r"=(.*)$", metadata, re.M)
        if len(values) != 1:
            raise signing.SigningError("Missing or ambiguous saved prepared tag identity.")
        expected[key] = values[0]
    if (expected["tag"] != tag or
            any(not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", expected[key])
                for key in ("tagObject", "commit"))):
        raise signing.SigningError("Invalid saved prepared tag identity.")
    ref = "refs/tags/" + tag
    local_object = signing.run(["git", "rev-parse", "--verify", ref]).strip()
    local_commit = signing.run(["git", "rev-parse", "--verify", ref + "^{commit}"]).strip()
    if local_object != expected["tagObject"] or local_commit != expected["commit"]:
        raise signing.SigningError("Prepared release tag changed locally; no publication attempted.")
    try:
        remote = signing.run(["git", "ls-remote", "--exit-code", "origin", ref, ref + "^{}"])
    except signing.SigningError:
        raise signing.SigningError(
            "Could not verify prepared release tag on origin; no publication attempted.") from None
    refs = dict(line.split()[::-1] for line in remote.splitlines())
    remote_commit = refs.get(ref + "^{}", refs.get(ref))
    if refs.get(ref) != local_object or remote_commit != local_commit:
        raise signing.SigningError("Prepared release tag differs on origin; no publication attempted.")


def publish():
    """Verify the prepared signed assets and tag, then stage and publish a new release."""
    tag, _ = identity()
    files = artifacts()
    # Publication verifies existing signatures with public material only.
    if any(not Path(str(path) + ".asc").is_file() for path in files):
        raise signing.SigningError("Sign and verify both GitHub assets before publication.")
    signing.sign_files(files)
    verify_remote_tag(tag)
    assets = [str(item) for path in files for item in (path, str(path) + ".asc")]
    query = ("query($owner:String!,$name:String!,$tag:String!){"
             "repository(owner:$owner,name:$name){release(tagName:$tag){id}}}")
    try:
        result = json.loads(signing.run(["gh", "api", "graphql", "-f", "query=" + query,
                                        "-F", "owner={owner}", "-F", "name={repo}", "-f", "tag=" + tag]))
    except (signing.SigningError, ValueError):
        raise signing.SigningError("Could not inspect GitHub release; no publication attempted.") from None
    data = result.get("data") if isinstance(result, dict) else None
    repository = data.get("repository") if isinstance(data, dict) else None
    if not isinstance(repository, dict) or "release" not in repository or result.get("errors"):
        raise signing.SigningError("Could not inspect GitHub release; no publication attempted.")
    if repository["release"] is not None:
        raise signing.SigningError("GitHub release or draft already exists; inspect and recover it manually.")
    signing.run(["gh", "release", "create", tag, *assets, "--verify-tag", "--draft",
                 "--title", tag, "--generate-notes"], timeout=1800)
    # A failed create/upload leaves its draft unpublished for manual recovery.
    signing.run(["gh", "release", "edit", tag, "--draft=false"], timeout=1800)


def main():
    try:
        command = sys.argv[1:]
        if command == ["check-tokens"]:
            check_tokens()
        elif command == ["identity"]:
            tag, version = identity()
            print(f"RELEASE_TAG={tag}\nRELEASE_VERSION={version}")
        elif command == ["sign"]:
            signing.sign_files(artifacts())
        elif command == ["publish"]:
            publish()
        else:
            raise signing.SigningError("Expected check-tokens, identity, sign, or publish.")
        return 0
    except signing.SigningError as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print("Release asset operation failed; values withheld.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
