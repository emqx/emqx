# Contributing

You are welcome to submit any bugs, issues and feature requests on this repository.

## Before You Open a Pull Request

Pull requests are welcome.

Start with an issue that carries the
[`help wanted`](https://github.com/emqx/emqx/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)
label. A maintainer has read that issue, has agreed on the change, and has decided that it does
not need internal context that is not in the issue. Those issues are ready for you to take. Add a
comment to say that you are working on it, so that two people do not write the same fix.

For an issue without the label, **add a comment before you write the code**. Describe the change
that you plan. Wait for a maintainer to answer.

This is not a formality. An issue without the label can have a reason that the text does not give:

- The fix belongs in a dependency, or in another repository, so a change here cannot correct it.
- The behaviour is intentional, and the issue is a request to document it.
- Someone already works on it, or it is part of a larger change that is in progress.
- The correct fix needs a design decision that a maintainer must make first.

A short comment costs you one day. A pull request that we cannot merge costs you the whole change.

A small correction needs no discussion. Send a pull request directly for a typo, a broken link,
or a documentation error.

## Which Branch to Target

EMQX keeps active development on several `dev-XX` branches at once, one per still-supported
release line (for example `dev-58`, `dev-510`, `dev-63`). Each `dev-XX` branch syncs forward
automatically into the next branch in the chain, so a change merged into an earlier branch reaches
every later branch without a separate pull request.

- **Target the earliest active `dev-XX` branch the bug affects**, especially for high-severity
  fixes. This is what lets the fix reach every supported release line downstream through the sync
  chain.
- **If you are not sure which branch is earliest or still active, targeting the latest `dev-XX`
  branch is fine.** Say so in the pull request description and ask for guidance. A maintainer can
  redirect the pull request to an earlier branch during review.
- **Do not open the same pull request against multiple `dev-XX` branches.** The sync chain already
  moves the change forward. Duplicate pull requests create duplicate review work, and they can
  diverge when they are not merged at the same time.
- Never open a pull request against a `release-XX` branch directly. Those branches are
  fast-forwarded automatically from the matching `dev-XX` branch and do not accept direct pushes.
- Never open a pull request against `master`. It is the last branch in the forward-sync chain and
  must stay fast-forwardable from the newest `dev-XX` branch. A direct commit makes the next sync
  fail, and a maintainer must then repair the chain by hand. Your change reaches `master` through
  the chain.

The 5.x and 6.x lines are separate chains. A change merged into `dev-58` reaches `dev-59` and
`dev-510`. It does not reach `dev-60`. When a change applies to both lines, open one pull request
against the earliest branch of each chain.

## Commit Message Guidelines

We have very precise rules over how our git commit messages can be formatted. This leads to **more readable messages** that are easy to follow when looking through the **project history**.

### Commit Message Format

Each commit message consists of a **header**, a **body** and a **footer**. The header has a special format that includes a **type**, a **scope** and a **subject**:

```
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

The **header** with **type** is mandatory. The **scope** of the header is optional. This repository has no predefined scopes. A custom scope can be used for clarity if desired.

Any line of the commit message cannot be longer 100 characters! This allows the message to be easier to read on GitHub as well as in various git tools.

The footer should contain a [closing reference to an issue](https://help.github.com/articles/closing-issues-via-commit-messages/) if any.

Example 1:

```
feat: add Fuji release compose files
```

```
fix(script): correct run script to use the right ports

Previously device services used wrong port numbers. This commit fixes the port numbers to use the latest port numbers.

Closes: #123, #245, #992
```

### Revert

If the commit reverts a previous commit, it should begin with `revert: `, followed by the header of the reverted commit. In the body it should say: `This reverts commit <hash>.`, where the hash is the SHA of the commit being reverted.

### Type

Must be one of the following:

- **feat**: New feature for the user, not a new feature for build script
- **fix**: Bug fix for the user, not a fix to a build script
- **docs**: Documentation only changes
- **style**: Formatting, missing semi colons, etc; no production code change
- **refactor**: Refactoring production code, eg. renaming a variable
- **chore**: Updating grunt tasks etc; no production code change
- **perf**: A code change that improves performance
- **test**: Adding missing tests, refactoring tests; no production code change
- **build**: Changes that affect the CI/CD pipeline or build system or external dependencies (example scopes: jenkins, makefile)
- **ci**: Changes provided by DevOps for CI purposes.
- **revert**: Reverts a previous commit.

### Scope

There are no predefined scopes for this repository. A custom scope can be provided for clarity.

### Subject

The subject contains a succinct description of the change:

- use the imperative, present tense: "change" not "changed" nor "changes"
- don't capitalize the first letter
- no dot (.) at the end

### Body

Just as in the **subject**, use the imperative, present tense: "change" not "changed" nor "changes". The body should include the motivation for the change and contrast this with previous behavior.

### Footer

The footer should contain any information about **Breaking Changes** and is also the place to reference GitHub issues that this commit **Closes**.

**Breaking Changes** should start with the word `BREAKING CHANGE:` with a space or two newlines. The rest of the commit message is then used for this.

## Changelog

Changes affecting EMQX functionality shall be described in a separate markdown file under `changes` directory.

File name pattern: `changes/(ce|ee)/(feat|perf|fix)-<PR-id>.en.md`, where:

- `ce,ee`: Indicates whether given change affects community and enterprise edition (`ce`), or enterprise edition only (`ee`); for any change only one file is needed as enterprise edition absorbs all changes from the community edition automatically. When in doubts, one could consult [documentation](https://www.emqx.io/docs/en/latest/). Enterprise features have a corresponding "Tip" banner, see for example [here](https://www.emqx.io/docs/en/v5.1/data-integration/data-bridge-influxdb.html).
- `feat|perf|fix`: Whether the change is a new functionality (`feat`), performance improvement (`perf`), or a bug fix (`fix`).
- `PR-id`: Github pull request id. Since pull request id cannot be known before the PR is actually created, it's common to add change log entry in a separate commit.
- `en`: ISO 639-1 language code indicating the language the change log entry is written in. Right now we are only accepting entries in English.

## Release Version

Every pull request template has a `Release version:` field. It lists **every** version that will
contain the change: the next release on the base branch, and the next release on each downstream
branch that the base branch fast-forwards into. It is not only the base branch version.

Run `./scripts/rel-versions` in your checkout to get the list. A line with no GA release yet shows
as `M.N.0`.

For a pull request based on `dev-58`, the script prints one comma-separated line:

```
5.8.12, 5.9.4, 5.10.6
```

Put each of those versions in the `Release version:` field.

The same value sets `introduced_in` in a BPAPI proto module, and it sets the scope of a changelog
entry. Always run the script. Do not guess a single version.

Two facts that the script does not give you:

- **There is no 6.4 release.** The 6.x line ends at `dev-63`, which syncs forward into `dev-70`.
  A `6.4.*` version does not exist. Never write one in a changelog, an `introduced_in` value or a
  `Release version:` field. The full v6 chain is
  `dev-60` → `dev-61` → `dev-62` → `dev-63` → `dev-70` → `master`.
- **The git tag prefix changed at v6.** A release tag on a 5.x line has an `e` prefix, for example
  `e5.8.12` or `e5.10.4`. From 6.0 there is no prefix, for example `6.0.3` or `6.3.0`. The prefix
  is part of the tag string. It is not a marker for the version family.

### Sources that name a stale branch

Two scripts predate this branch model and still name `release-5X` as a working branch:
`scripts/rel/cut.sh` and `scripts/rel/sync-remotes.sh` on the 5.x branches. Do not use them to
select a pull request base.

## Build Toolchain

### Use the EMQX fork of Erlang/OTP

EMQX builds against the EMQX fork of Erlang/OTP at
[emqx/otp](https://github.com/emqx/otp). **A build with an upstream Erlang/OTP release is not
supported.** The fork carries patches that EMQX depends on, and a build against an upstream
release can fail to compile, or can compile and then behave differently at run time.

`.tool-versions` in the repository root gives the exact version:

```
erlang 26.2.5.14-1
```

The trailing number is the fork revision. It does not exist upstream, so no upstream
installation matches the version that this branch needs. The matching fork tag is
`OTP-26.2.5.14-1`.

Each branch pins its own version. Read `.tool-versions` on the branch that you work on. Do not
copy the version from another branch.

### Use an emqx-builder image

The simplest way to get a correct toolchain is the `emqx-builder` Docker image. Each image
contains the forked OTP, Elixir, Rust and the other build tools, at the versions that CI uses.
This removes the whole question of what to install on your own machine.

`show-latest-images.sh` in the root of [emqx-builder](https://github.com/emqx/emqx-builder)
prints the available image tag for each supported operating system. A tag has this form:

```
ghcr.io/emqx/emqx-builder/<builder-tag>:<elixir>-<otp>-<os>
```

Select the image whose `<otp>` matches the `.tool-versions` value on your branch, and whose
`<os>` matches the system that you build for. The images are built for each supported
architecture, so Docker selects the correct one for your machine.

Build in the container:

```bash
docker run --rm -it -v "$PWD:/emqx" -w /emqx \
  ghcr.io/emqx/emqx-builder/<builder-tag>:<elixir>-<otp>-<os> \
  make
```

The CI workflows use the same images, so a build that succeeds in the container matches what CI
does.

## Code Style

### Running the checks

```
make fmt-diff                                        # format changed .erl files
make fmt                                             # whole tree, what CI runs
./scripts/elvis-check.sh $(git merge-base origin/<base> HEAD)
./scripts/apps-version-check.sh --auto-fix           # app vsn and plugin VERSION bumps
make static_checks                                   # includes the BPAPI baselines
```

`make fmt-diff` rewrites changed `.erl` files only. It does **not** touch `.hrl`, `.app.src`,
`rebar.config`, `.eterm`, `.escript` or `mix.exs`. CI runs `make fmt` on the whole tree and fails
on any difference, so run the full formatter when you add a file.

`elvis-check.sh` takes a commit SHA, not a branch name, and it checks every changed file, test
files included. These two rules fail most frequently: inconsistent variable casing in one module
(`QoS`, never `Qos`), and a line longer than 120 characters.

Commit the files that the formatter rewrites. Do not commit a reformatted file that your branch
does not change for another reason.

### Erlang style

- CI builds with `warnings_as_errors`. An unused variable, an unused import and a conflict between
  an import and a local function are compile errors. An incremental local build can miss these
  because of cached beam files, so read the warnings.
- Do not nest more than two levels. Write a helper function instead.
- Use `maybe ... end` for a sequence of checks that can each fail.
- Use the `-doc` and `-moduledoc` attributes. Do not use `@doc`.
- Give each test case function (`t_*`) a `-doc` that says what the case tests.

### Multi-line strings

On `dev-60` and each branch after it, write a string literal as a triple-quoted `"""` block when
the literal covers more than one line, or when it contains a `"` or an escape that a single-line
literal needs. Do not join `"..."` fragments, and do not put `\n` in a single-line literal. This
applies to a `-doc` body, a log or error template, help text, and embedded JSON or HOCON.

Put the closing `"""` on its own line, at the same indentation as the opening one. That
indentation is removed from each line of the content. A literal that is genuinely one line stays a
plain `"..."`.

The `-doc` attribute and the triple-quoted string both need OTP 27 or later, so availability
follows the OTP version in `.tool-versions`:

| Branches | OTP |
|---|---|
| `dev-58` | 26. Neither feature is available. |
| `dev-59`, `dev-510`, `dev-60` | 27 |
| `dev-61` and later | 28 |

When you backport a change to `dev-58`, convert each `-doc` block to a `%%` comment, and convert
each multi-line literal to the form that the branch already uses.

## BPAPI Proto Modules

A proto module (`*_proto_v*.erl`) wraps a cluster-wide RPC and implements the `emqx_bpapi`
behaviour. It is in a `proto/` subdirectory next to the module that it serves.

- **A released proto module version is frozen.** Never change one.
- To add an RPC function, create `_proto_v(N+1).erl`.
- `introduced_in/0` returns the release version string. See *Release Version* above.
- After you add or change a proto module, run `make static_checks` and commit the updated
  `apps/emqx/priv/bpapi.versions`.
- Never call `erpc` directly. Send each cluster RPC through a `_proto_v*.erl` module.

**A proto module is a local wrapper. The function that it calls is the contract.** You can delete
a superseded wrapper only while each function that it called still exists and stays exported,
because a node that runs an older release calls that function by name. If you remove a function
that looks unused in the current tree, you can break a rolling upgrade.

## Redacted Secrets Must Round-Trip

A `GET` that returns a secret masked as `******` must accept the same body back on `PUT` or `POST`.
A dashboard or a script reads a configuration, changes one unrelated field, and writes the whole
configuration back. An unchanged masked value therefore means "keep the stored secret". It must
never mean "set the secret to the literal `******`".

- Use `emqx_utils:deobfuscate(NewConf, OldConf)` on the update path. These modules already call it:
  `emqx_bridge_v2_api`, `emqx_cluster_link_api`, `emqx_exhook_api`, `emqx_ai_completion_api`,
  `emqx_dashboard_sso_manager`.
- Schema validation of the request body runs **before** the handler, so de-obfuscation cannot
  rescue a value that the schema rejects. A mask works for a plain string, because `******` is a
  valid binary. It fails when the sensitive field is a union, a struct, an enum or a number. The
  mask gives a value of the wrong *type*, and the request fails before the handler runs.
- Redact the sensitive **leaf inside a structure** instead of the whole field. The shape then stays
  valid, and `deobfuscate/2` can do its work.
- Keep an intentional removal different from "unchanged". `******` means unchanged. An explicit
  empty value or `none` means remove.
- Test the round trip, not only the mask. Send a `GET`, send the same body back with `PUT`, and
  assert that the request succeeds and that the stored secret did not change. Separately, assert
  that `GET` still masks the secret.

## i18n

- Edit `rel/i18n/*.hocon`. Each file has the name of its schema module.
- Never edit `apps/emqx_dashboard/priv/desc.en.hocon` or `desc.zh.hocon`. They are generated.

## Writing

We prefer [ASD-STE100](https://asd-ste100.org/) (Simplified Technical English) for the wording and
the phrasing of **code comments, pull request descriptions and review comments**. The same
preference applies to documentation, commit messages and changelog entries.

ASD-STE100 is a controlled-English specification for technical documentation. It is written for
readers who read the text in a second language. Many readers of this project do, so the style
helps here.

This is a preference. We do not reject a pull request because of its phrasing, and you do not need
to read the specification. Follow the points below:

- Write short declarative sentences, one idea in each, in the active voice.
- Use plain terms from the domain, and common verbs: check, run, send, match, fail, return.
- State a condition or a prohibition directly: "When X occurs, do Y." "A must not do B."
- Write an instruction in the imperative.
- Use the same term for the same thing in a document. Define an unfamiliar term when you first use
  it.
- Do not use a metaphor, an invented abstraction, or phrasing written for effect.
- Write again, in plainer language, each sentence that sounds clever.

In a review comment, state the change that you want and the reason for it. The author must not
have to infer either one. Write "This allocates on every message. Move the allocation out of the
loop." Do not write "this feels expensive".

A changelog entry describes **user-visible behaviour**, not implementation. Write what changed for
a person who operates EMQX.
