SHELL := bash
.ONESHELL:

RELEASE_GUARD ?= scripts/release/guard.py
RELEASE_SIGNING_HELPER ?= $(dir $(RELEASE_GUARD))signing.py
RELEASE_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
.SHELLFLAGS := -eo pipefail -c
# Defaults are exported for recursive release recipes.
export RELEASE_SKIP_TESTS ?= false
export RELEASE_VERSION ?=

.PHONY: github-release release-check-user release-check-tokens release-prepare release release-dry-run _release-prepare _release _release-dry-run test-release-signing

github-release: release-check-user
	python3 "$(dir $(RELEASE_GUARD))assets.py" publish

release-check-tokens:
	@python3 "$(dir $(RELEASE_GUARD))assets.py" check-tokens

release-check-user:
	@set -eu
	if [ "$${GITHUB_ACTIONS:-}" = true ]; then
		# The release environment authorizes workflow operators independently of the signer.
		exit 0
	fi
	if ! user=$$(gh api user --jq .login); then
		echo "Release requires GitHub user scylladb-publisher. Run gh auth login or gh auth switch." >&2
		exit 1
	fi
	if [ "$$user" != scylladb-publisher ]; then
		echo "Release requires GitHub user scylladb-publisher (current: $$user)." >&2
		exit 1
	fi

release-prepare: release-check-user
	@python3 "$(RELEASE_GUARD)" check
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _release-prepare

release: release-check-user release-check-tokens
	@python3 "$(RELEASE_GUARD)" check
	@python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _release

release-dry-run: release-check-user release-check-tokens
	@python3 "$(RELEASE_GUARD)" check
	@python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _release-dry-run

test-release-signing:
	python3 -m unittest discover -s scripts/release -p 'test_signing.py' -v

_release-prepare: release-check-user
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true -DskipIntegrationTests=true -Ddocker.skip=true"
	fi
	NEXT_RELEASE=()
	if [[ -n "$${RELEASE_VERSION}" ]]; then
		if [[ ! "$${RELEASE_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$$ ]]; then
			echo "Release version must be X.Y.Z" >&2
			exit 1
		fi
		NEXT_RELEASE=("-DreleaseVersion=$${RELEASE_VERSION}")
	fi
	export MAVEN_OPTS
	baseline=$$(git rev-parse HEAD)
	python3 "$(RELEASE_GUARD)" run mvn -ntp -B release:prepare -DpushChanges=false "$${NEXT_RELEASE[@]}"
	python3 "$(RELEASE_SIGNING_HELPER)" save-prepared "$$baseline"

_release: release-check-user release-check-tokens
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true -DskipIntegrationTests=true -Ddocker.skip=true"
	fi
	python3 "$(RELEASE_GUARD)" run mvn -ntp -B release:perform -Drelease.autopublish=true

_release-dry-run: release-check-user release-check-tokens
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true -DskipIntegrationTests=true -Ddocker.skip=true"
	fi
	python3 "$(RELEASE_GUARD)" run mvn -ntp -B release:perform -Drelease.autopublish=false
