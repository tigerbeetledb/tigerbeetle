# Releases

How a new TigerBeetle release is made. Note that the process is being
established, so this document might not perfectly reflect reality just yet.

This document _starts_ with a succinct release manager algorithm for convenience of release managers.
The motivation for specific steps follows after.

## Release Manager Algorithm

1. Open [devhub](https://tigerbeetle.github.io/tigerbeetle/) to check that:
   - you are the release manager for the week
   - that the vopr results look reasonable (no failures and a bunch of successful runs for recent
     commits)

2. ```console
   $ ./zig/zig build scripts -- changelog
   ```
   This will update local repository to match remote, create a branch for changelog PR, and add a
   scaffold of the new changelog to CHANGELOG.md. Importantly, the scaffold will contain a new
   version number with patch version incremented:

   ```
   ## TigerBeetle 0.16.3   <- Double check this version.

    Released 2024-08-29

    - [#2256](https://github.com/tigerbeetle/tigerbeetle/pull/2256)
          Build: Check zig version
    - [#2248](https://github.com/tigerbeetle/tigerbeetle/pull/2248)
          vopr: heal *both* wal header sectors before replica startup

    ### Safety And Performance

    -

    ### Features

    -

    ### Internals

    -

    ### TigerTracks 🎧

    - []()
    ```

    If the current release is being skipped, replace the header with `## TigerBeetle (unreleased)`.

3. Fill in the changelog:
   - categorize pull requests into three buckets.
   - drop minor pull requests
   - group related PRs into a single bullet point
   - double-check that the version looks right
   - if there are any big features in the release, write about them in the lead paragraph.
   - pick the tiger track!

4. Commit the changelog and submit a pull request for review

5. After the PR is merged, push to the `release` branch:

   ```console
   $ git fetch origin && git push origin origin/main:release
   ```
6. Ask someone else to approve the GitHub workflow.

7. Ping release manager for the next week in Slack.

### Error Handling

If the release failed completely (nothing was published), it is safe to re-run the release GitHub
Actions job.

More likely, the release was partially successful: eg, the NodeJS package was uploaded, but
uploading the Java package failed. This is not a problem --- the replica release on GitHub will remain a draft in
this case. To retry the release, increment the version number of the latest changelog entry alongside
any changes to fix the release process, and push the new commit to the release branch. The _old_
version number will be irrevocably burned. There will be an intentional gap in the changelog
sequence. When a release with the new version number is fully released, delete the draft release on
GitHub.

It could also be the case that a release was successful, but some issue with the code is discovered
and a quick fix is necessary. The preferred approach is to do a normal fix-forward release. While
releases are normally weekly, it is possible to do several releases in a single day. Note that a
fix-forward release would go through the normal VSR upgrade protocol.

Finally, if the release is bad _and_ the normal upgrade protocol doesn't work (e.g., a replica
crashes on startup immediately), it is possible to make a release which uses the same VSR release
triple and is considered to be the "same" release from the perspective of the protocol. In this
case, the git tag and the VSR release in the binary would differ. To make this kind of release,
adjust `version_info.release_triple` manually in `release.zig`.

## What Is a Release?

TigerBeetle is distributed in binary form. There are two main reasons for this:

- to ensure correctness and rule out large classes of configuration errors, the
  actual machine code must be tested.
- Zig is not stable yet. Binary releases insulate the users from this
  instability and keep the language as an implementation detail.

TigerBeetle binary is released in lockstep with client libraries. At the moment,
implementation of the client libraries is tightly integrated and shares code
with TigerBeetle, requiring matching versions.

Canonical form of the "release" is a `dist/` folder with the following
artifacts:

- `tigerbeetle/` subdirectory with `.zip` archived `tigerbeetle` binaries built
  for all supported architectures.
- `dotnet/` subdirectory with a Nuget package.
- `go/` subdirectory with the source code of the go client and precompiled
  native libraries for all supported platforms.
- `java/` subdirectory with a `.jar` file.
- `node/` subdirectory with a `.tgz` package for npm.

## Publishing

Release artifacts are uploaded to appropriate package registries. GitHub release
is used for synchronization:

- a draft release is created at the start of the publishing process,
- artifacts are uploaded to GitHub releases, npm, Maven Central, and Nuget. For Go, a new commit is
  pushed to <https://github.com/tigerbeetle/tigerbeetle-go>. Similarly, docs are uploaded to
  <https://github.com/tigerbeetle/docs>.
- if publishing to all registries were successfully, the release is marked as
  non-draft.

All publishing keys are stored as GitHub Actions in the `release` environment. For Go and docs,
a personal access token is used. These tokens expire after a year, to refresh a token:

- Create fine grained personal access token, PAT, using your personal GitHub account ([GitHub
  documentation](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-fine-grained-personal-access-token))
- Scope the token to the tigerbeetle github organization.
- Grant write access to the relevant repo (use separate tokens for different repositories).
- Update the token in the `release` environment in the `tigerbeetle` repository.

## Versioning

Because releases are frequent, we avoid specifying the version in the source
code. The source of truth for version is the CHANGELOG.md file. The version at
the top becomes the version of the new release.

Version numbers are monotonic, but can have gaps, see the error handling part of release manager
algorithm.

## Changelog

Purposes of the changelog:

- For everyone: give project a visible "pulse".
- For TigerBeetle developers: tell fine grained project evolution story, form
  shared context, provide material for the monthly newsletter.
- For TigerBeetle users: inform about all visible and potentially relevant
  changes.

As such:

- Consider skipping over trivial changes in the changelog.
- Don't skip over meaningful changes of the internals, even if they are not
  externally visible.
- If there is a story behind a series of pull requests, tell it.
- And don't forget the TigerTrack of the week!

## Release Logistics

Releases are triggered manually, on Monday. Default release rotation is on the
devhub: <https://tigerbeetle.github.io/tigerbeetle/>.

The middle name is the default release manager for the _current_ week. They should execute [Release
Manager Algorithm](#release-manager-algorithm) on Monday. If the release manager isn't available on
Monday, a volunteer picks up that release.

## Skipping Release

Because releases are frequent, it is not a problem to skip a single release. In fact, allowing to
easily skip a release is one of the explicit purposes of the present process.

If there's any pull request that we feel pressured should land in the next release, the default
response is to land the PR under its natural pace, and skip the release instead.

Similarly, if there's a question of whether we should do a release or to skip one, the default
answer is to skip. Skipping is cheap!

If the release is skipped, the changelog is still written and merged on Monday, using the following
header: `## TigerBeetle (unreleased)`.

When the next real release happens, it should merge all the previously unreleased changes into a
single versioned changelog entry, to inform users making upgrades.
