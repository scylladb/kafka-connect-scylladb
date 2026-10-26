# Release notes

The Release workflow creates generated GitHub release notes and uploads the JAR
and Confluent ZIP after Maven publication and the release tag push. It uses the
exact `scm.tag` prepared by Maven (for example `1.1.11`), records it before Maven
cleans up `release.properties`, and checks that the remote
tag matches. Existing published notes are preserved and artifacts are replaced
on retry; an existing draft fails visibly for a maintainer to finish deliberately.
All signing, release blockers, credential checks and notifications remain in the
release process. Do not rerun the whole Release workflow to retry an upload:
`release:prepare` cuts a new version.

`.github/release.yml` excludes PRs carrying `omit-from-release-notes`.
`actions/labeler` uses `.github/labeler.yml` to apply this label only when all
reported changed filenames match known documentation,
`src/test/`, `.github/`, release guard tests, `.gitignore`, or Renovate config.
README and Markdown guides in `documentation/` qualify even when packaged with
the connector. The logos in `images/` are Confluent Hub package assets and remain
visible. Mixed PRs, `pom.xml`, production code/resources,
release helper implementation, executable scripts, deployment config, packaging
changes and unknown paths remain visible. Safeguards check complete, stable file
listings and the original paths of renames as well as their destinations. Moving
production code into a support path must not hide its removal from production.
Author, title, `chore` and `dependencies` labels do not determine omission.

Labeling runs on open, synchronize, reopen, edit and close. The pinned action
uses trusted workflow configuration and helper code without checking out or
executing PR head code.
`sync-labels: true` removes the omission label when filename rules stop matching.
A single brace-union pattern must match every reported file, so combinations of
documentation, tests and CI still qualify. Runs for a PR are serialized, including
manual labeling. Once the target is confirmed as a PR, failed or skipped
classification attempts to remove unverified omission labels; unconfirmed targets
receive no label writes. API outages or a canceled runner can prevent cleanup, and
before/after checks cannot make classification atomic.

Before generating notes, classify already merged PRs in Actions → Classify release
notes → Run workflow on `master`, entering a PR number, or run:

```sh
gh workflow run label-release-notes.yml --ref master -f pull-request=123
```

Manual dispatch from any other ref fails before helper checkout or dependency
installation. This enforces the shipped workflow's default-branch procedure;
it cannot secure a deliberately modified workflow on another branch.

Identify the merged PRs in the intended release range, from the previous
published tag through the exact prepared tag, before starting verification.
For every merged PR covered by the release, inspect its current changed-file
list, including rename origins, and confirm that the classifier verified that
same revision and complete file list. Check the resulting label: support-only
changes qualify, while any production or unknown path must remain visible. A
green workflow alone does not establish that these facts are still current.
Rerun classification after changes, and investigate any failed or skipped run
or unverified omission label before proceeding.

Release publication remains asynchronous: it does not wait for classification.
This manual check is required before generating notes. If it is missed, notes
can retain omitted production changes or unwanted support entries. This PR,
including later review cycles and resumed work, excludes an automated release
barrier; no follow-up synchronization work is scheduled. Preserve already
published notes when retrying artifact uploads.

Maintainers can apply the label manually after verifying every change is
support-only; the next classifier run reconciles it with these rules. Manual
addition or removal of this exact label skips the local Jira label callers.
Other events retain their existing routing. Existing Jira label copies and
other synchronization paths are unchanged.
