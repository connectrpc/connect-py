# Releasing Connect

This document outlines how to create a release of Connect.

1. Clone the repo, ensuring you have the latest main.

2. On a new branch, update version strings with the `bump` command, either to the next minor or patch version, based on the changes that are included in this new release.

- If there are only bug fixes and no new features, run `uv run poe bump --version patch`.
- If there are features being released (or breaking changes being made while we're pre-1.0 release), run `uv run poe bump --version minor`.

Note the new version X.Y.Z in the updated files.

3. Open a PR titled "Prepare for vX.Y.Z" ([Example PR: #60](https://github.com/connectrpc/connect-py/pull/60)) and assign the `connectrpc/python` group as reviewers. Once it's approved by at
   least one other maintainer and CI passes, merge it.

   _Make sure no new commits are merged until the release is complete._

4. Review all commits in the new release and for each PR check an appropriate label is used and edit the title to be meaningful to end users.

5. Using the GitHub UI, create a new release.

   - Under “Select tag”, type in “vX.Y.Z” to create a new tag for the release upon publish.
   - Target the main branch.
   - Title the Release “vX.Y.Z”.
   - Click “set as latest release”.
   - Set the last version as the “Previous tag”.
   - Click “Generate release notes” to autogenerate release notes.
   - Edit the release notes. A summary and other categories should be added, in most cases, with the two categories ### Enhancements and ### Bugfixes. Feel free to collect multiple small changes to docs or GitHub config into one line, but try to tag every contributor. Make especially sure to credit new external contributors!

6. Publish the release.

## Releasing `connectrpc-otel` and `connectrpc-grpcreflect`

These packages are versioned and released independently of `connectrpc`. Follow the steps above, with these differences:

- Bump the version with `uv version --package connectrpc-otel --bump <patch|minor>` (or `--package connectrpc-grpcreflect`) instead of `poe bump`.
- Title the PR "Prepare for connectrpc-otel vX.Y.Z" ([Example PR: #277](https://github.com/connectrpc/connect-py/pull/277)).
- Tag the release `connectrpc-otel/vX.Y.Z` (or `connectrpc-grpcreflect/vX.Y.Z`) and title it "otel vX.Y.Z" (or "grpcreflect vX.Y.Z"). The tag prefix triggers that package's release workflow.
- When generating release notes, set the package's previous tag as the "Previous tag", and leave "set as latest release" unchecked so the `connectrpc` release stays latest.
