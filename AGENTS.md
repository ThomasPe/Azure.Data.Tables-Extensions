# AGENTS.md

Instructions for AI coding agents working in this repository.

## Release process — read before touching version/changelog files

Releases are fully automated by [release-please](https://github.com/googleapis/release-please)
(`.github/workflows/main.yml`, `release-please-config.json`, `.release-please-manifest.json`).
**Do not hand-edit these as part of a feature/fix change:**

- Do not bump `Version`, `AssemblyVersion`, or `FileVersion` in `src/Directory.Build.props`.
  release-please writes these via its `extra-files` XML updater when it opens/updates the
  release PR.
- Do not add entries to `CHANGELOG.md` by hand. release-please generates them from commit
  messages when it opens/updates the release PR.
- Do not create git tags or GitHub Releases manually for routine releases; merging the
  release-please PR does that.

### What you *do* need to get right: commit messages

This repo merges PRs with a real merge commit (not squash), so every individual commit you
make stays in history and is scanned individually — not just the PR title. Every commit that
should influence the next release must follow
[Conventional Commits](https://www.conventionalcommits.org/):

- `fix: ...` → patch bump
- `feat: ...` → minor bump
- `feat!: ...`, `fix!: ...`, or a `BREAKING CHANGE:` footer → major bump
- `chore:`, `docs:`, `refactor:`, `test:`, `ci:` etc. → included in history but do not trigger
  a release on their own

Commits without a Conventional Commit prefix are effectively invisible to release-please —
they won't trigger a release PR and won't produce a changelog line. If a PR contains only
non-conventional commits, no release will be proposed for it, which is often fine (e.g. pure
CI/docs changes) but should be a deliberate outcome, not a surprise.

### How a release actually ships

1. Conventional Commits land on `main` (via merged PRs).
2. The `release-please` job (`.github/workflows/main.yml`) opens or updates a release PR
   containing the computed version bump and generated `CHANGELOG.md` entry.
3. A human merges that release PR. This creates the git tag and GitHub Release.
4. The `publish` job then packs both projects and pushes to NuGet using trusted publishing
   (OIDC via `NuGet/login@v1` — no API key secret involved).

If you're asked to prepare a release-worthy change, make sure the commit message(s) reflect
the right Conventional Commit type — that's the only lever that controls versioning now.
