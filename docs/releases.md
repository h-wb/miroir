# Releases

Only the collector is released. It is a program installed on personal machines,
so a machine has to be able to name the version it runs. The server is not
released: it ships as a container image named after its commit, and the
deployment pins that image.

A collector release is a Git tag `collector-vX.Y.Z` on main with a GitHub
release beside it. Nothing is built or uploaded: a machine installs the tagged
source (see [the collector's README](../collector/README.md)), and the list of
GitHub releases is what tools such as mise read to know which versions exist.

## Version policy

PR titles follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/),
and PRs are squash merged, so each PR contributes one classified commit. A
commit counts towards a collector release when it changes a file under
`collector/`.

| Change | Before 1.0 | From 1.0 |
| --- | --- | --- |
| `fix`, `perf` | Patch | Patch |
| `feat` | Minor | Minor |
| `!` or `BREAKING CHANGE:` | Minor | Major |
| `docs`, `test`, `ci`, `chore`, `build`, `refactor` | No release on their own | No release on their own |

Mark a change breaking when a machine has to do something to keep working: a
renamed configuration key or command, a state folder an older version cannot
read, a batch format an older server refuses. Say what to do in the PR body; it
becomes the release note.

A batch format carries its own version and the server refuses what it does not
know (decision 031), so a collector and a server that disagree fail at delivery
and never load wrong rows. A release that needs a newer server says so in its
notes.

## Cutting a release

After a successful **Test** run on main, Release Please opens or updates one
release pull request. It bumps the version in
`collector/pyproject.toml` and `collector/miroir_collector/__init__.py` and
writes `collector/CHANGELOG.md` from the commits since the last release.

1. Read the proposed version and notes; edit the notes in the PR if needed.
2. Merge it when its checks pass.
3. After main's Test run passes, Release Please creates the tag and the GitHub
   release.

Nothing is deployed by this. A machine moves when its own configuration names
the new tag.

Do not create a tag or a release by hand, and do not edit the version by hand:
the manifest (`.release-please-manifest.json`) would no longer match.

## Maintainer setup (once)

1. Allow squash merges in the repository settings and use the PR title as the
   squash commit title.
2. Create `RELEASE_PLEASE_TOKEN` as an Actions secret: a fine-grained token from
   your account or a bot account, scoped to this repository, with Contents,
   Issues and Pull requests read and write. Release Please needs its own token
   because events created with the default workflow token do not start other
   workflows, so the release PR would get no checks.
3. Set the Actions variable `RELEASE_AUTOMATION_ENABLED` to `true`. The workflow
   does nothing until then, including on forks.
4. Add the workflow `.github/workflows/release-please.yml`.

The first release counts commits made after the commit named `bootstrap-sha` in
`release-please-config.json`, starting from version 0.1.0.
