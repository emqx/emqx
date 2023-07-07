# Contributing

You are welcome to submit any bugs, issues and feature requests on this repository.

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
