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
