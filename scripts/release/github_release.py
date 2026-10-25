"""Publish the prepared Maven release's GitHub notes and existing artifacts."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys


def command(*args, check=True):
    return subprocess.run(args, text=True, stdout=subprocess.PIPE,
                          stderr=None if check else subprocess.PIPE, check=check)


def release_tag():
    # Use Maven's exact prepared tag, never the nearest ancestor returned by describe.
    recorded = os.environ.get('RELEASE_TAG')
    tags = ([recorded] if recorded is not None else
            re.findall(r'^scm\.tag=(.+)$', Path('release.properties').read_text(), re.M))
    if len(tags) != 1 or not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', tags[0]):
        raise RuntimeError('Expected one plain version scm.tag in release.properties')
    return tags[0]


def record():
    tag = release_tag()
    tag_object = command('git', 'rev-parse', '--verify', f'refs/tags/{tag}').stdout.strip()
    commit = command('git', 'rev-parse', '--verify', f'refs/tags/{tag}^{{commit}}').stdout.strip()
    if not all(re.fullmatch(r'[0-9a-f]{40,64}', value) for value in (tag_object, commit)):
        raise RuntimeError('Invalid prepared tag identity')
    # Maven removes its descriptor after a successful perform. Keep the exact
    # prepared identity across that cleanup and verify it again before publishing.
    with open(os.environ['GITHUB_ENV'], 'a') as environment:
        environment.write(f'RELEASE_TAG={tag}\nRELEASE_TAG_OBJECT={tag_object}\nRELEASE_COMMIT={commit}\n')


def publish():
    tag = release_tag()
    repo = os.environ['GITHUB_REPOSITORY']
    local = command('git', 'rev-parse', '--verify', f'refs/tags/{tag}^{{commit}}').stdout.strip()
    tag_object = command('git', 'rev-parse', '--verify', f'refs/tags/{tag}').stdout.strip()
    if (os.environ.get('RELEASE_COMMIT', local) != local or
            os.environ.get('RELEASE_TAG_OBJECT', tag_object) != tag_object):
        raise RuntimeError('Prepared release tag changed after Maven prepare')
    remote = command('git', 'ls-remote', '--exit-code', 'origin',
                     f'refs/tags/{tag}', f'refs/tags/{tag}^{{}}').stdout.splitlines()
    refs = dict(line.split()[::-1] for line in remote)
    remote_commit = refs.get(f'refs/tags/{tag}^{{}}', refs.get(f'refs/tags/{tag}'))
    if remote_commit != local or refs.get(f'refs/tags/{tag}') != tag_object:
        raise RuntimeError('Prepared release tag is absent or differs on origin')
    jar = f'target/checkout/target/kafka-connect-scylladb-{tag}.jar'
    zip_file = f'target/checkout/target/components/packages/scylladb-kafka-connect-scylladb-{tag}.zip'
    for artifact in (jar, zip_file):
        if not Path(artifact).is_file():
            raise RuntimeError(f'Missing release artifact: {artifact}')
    existing = command('gh', 'release', 'view', tag, '--repo', repo,
                       '--json', 'isDraft,tagName', check=False)
    if existing.returncode == 0:
        release = json.loads(existing.stdout)
        if release['tagName'] != tag or release['isDraft']:
            raise RuntimeError('Existing release is a draft or has an unexpected tag; finish it explicitly')
        command('gh', 'release', 'upload', tag, jar, zip_file, '--repo', repo, '--clobber')
    else:
        # --verify-tag also prevents accidental tag creation. API failures remain
        # visible: creation fails if a release already exists or GitHub is unavailable.
        command('gh', 'release', 'create', tag, jar, zip_file, '--repo', repo,
                '--verify-tag', '--title', tag, '--generate-notes')


if __name__ == '__main__':
    if sys.argv[1:] == ['--record']:
        record()
    elif sys.argv[1:] == ['--tag']:
        print(release_tag())
    else:
        publish()
