# AGENTS.md

Instructions for coding agents working in this repository.

Read `CONTRIBUTING.md` first. It holds the rules that apply to every contributor, human or agent:
which branch to target, commit message format, the changelog, the `Release version:` field, code
style, BPAPI proto rules, and the i18n source of truth. **Those rules are not repeated here.**
This file adds only what an agent needs on top of them.

Nothing in this file is machine-specific. Personal paths, tooling and workflow belong in your own
agent config outside the repository, not here.

## Non-negotiable

- **Never open a pull request against a `release-*` branch, or against `master`.** Pass
  `--base dev-XX` to every `gh pr create`. A bot pushes `release-*`. `master` must stay
  fast-forwardable from the newest `dev-XX` branch. A direct commit on either branch breaks this.
  A change reaches both branches through the sync chain. See CONTRIBUTING.md.
- **Open every pull request as a draft.** Pass `--draft`. Marking a PR ready for review is a
  decision a human makes after reading the diff.
- **Stage files by explicit path.** Never `git add -A`, `git add .` or `git commit -a`. Working
  checkouts carry untracked scratch files, and blanket adds have put job logs, repro scripts and
  PDFs into real pull requests.
- **Never use `git checkout --ours <file>` or `--theirs <file>`** to resolve a conflict. They
  replace the whole file and discard hunks git had already merged correctly from the other side.
  Edit the conflict markers by hand, then `git add` the path.
- **No AI attribution anywhere.** No "Generated with ..." footer in a PR description, no
  session-link or co-author trailer in a commit message.
- **Never pass `--no-verify`.** The hooks exist to catch what CI will catch later.
- **Do not post GitHub comments or review replies** unless you were asked to. Address review
  feedback in code and commits, and report the comment-to-commit mapping to the person you are
  working with.

## Before you push

Run the repository's sanity checks locally and push only when they pass. They are the same checks
the `sanity-checks` CI job runs: commit titles, changelog file names, newline at EOF, app versions,
BPAPI baselines, API module scopes, the umbrella app list, whole-tree formatting, and elvis.

The individual commands are in CONTRIBUTING.md. Two failures cause most CI failures on
agent-authored branches:

- a missing app `vsn` bump. The version-check script has an `--auto-fix` option.
- elvis `consistent_variable_casing` and lines over 120 characters, including in test files.

Elvis checks **every file changed against the merge base**, so a merge that brings in an untouched
file with a pre-existing violation will fail. Fix those in a separate follow-up commit and say so
in the PR body.

## Verify, do not assume

- When you claim a branch is or is not affected by a bug, check each branch rather than reasoning
  from the release numbering. Branches diverge, and a fix present on a later branch may be absent
  from an earlier one or the reverse.
- A new test must **fail before your change and pass after it**. Run it both ways and say so. A
  test that passes on the unfixed tree proves nothing.
- A suite that reports 0 failures can still have skipped every case. Check the skip count.
- Do not paste a command's expected output as if you had run it.

## Fix the class, not the instance

A report names one symptom. Look for the same defect elsewhere before you stop. A fix that corrects
the reported instance and leaves the others in place is an incomplete fix.

Write what you found in the PR body, also when you found no other instance. This tells the reviewer
that you looked.

Check these cases every time:

- **Related accessors that must agree.** Several functions can report the same property through
  different code paths. When you correct one function, the other functions continue to report a
  different result for the same object. When the decision belongs to the object, write one
  predicate and call it from each function.
- **Other callers of the same function.** When a function returns a new case that one caller does
  not handle, every other caller has the same defect. The return type is the contract. Check which
  other code depends on it.
- **A pattern that matches more than the reported input.** The reported value is one member of the
  set that the match accepts. List the set and test the other members.
- **Code that only looks unused.** Code with no caller in this tree can still be called from
  outside it, for example by an older node over RPC, by a plugin, or by a stored configuration.
  Search outside the tree before you delete it.
- **Every branch, not only the reported one.** Branches diverge. Check each branch instead of
  reasoning from the release numbering, and name the earliest affected branch in the PR.

### Security fixes

Apply the same rule, and audit two more cases: sibling code paths that guard the same boundary,
and duplicated authorization predicates that can drift apart.

**The security advisory is the only place that describes the defect.** A commit message, a pull
request description and a changelog entry are public from the moment you write them, and they stay
public. A reader who has the detail before the advisory is published, and before operators have
upgraded, can attack a running deployment.

So in a commit message, a pull request description, a changelog entry, a code comment and a branch
name:

- **Do not describe the defect.** No input that triggers it, no affected code path, no conditions
  that make a deployment vulnerable, no reproduction, no payload.
- **Do not state the effect.** No description of what a caller can read, write, bypass or crash.
- **Do not link the change to a security report.** No CVE number, no advisory link, no report id,
  and no reporter name, until the advisory is public.
- **Describe the change, not the defect.** "Validate the topic filter before the ACL check" says
  what the code now does. It does not say what happened before.
- Do not use alarm words, for example vulnerability, exploit, attacker or denial of service.

Put the full detail in the advisory. `SECURITY.md` has the reporting process. When you are not
sure how much a given text can say, write the minimum and ask the maintainer.

After the advisory is published, a later changelog or release note can reference it. That decision
belongs to the maintainer, not to you.

## Sync pull requests

A sync PR merges one `dev-XX` branch forward into the next. An incorrect conflict resolution
removes another person's change without a warning. Do the merge carefully.

1. Check for a fast-forward first. This is one command:
   ```
   git fetch origin dev-MN dev-M(N+1)
   git merge-base --is-ancestor origin/dev-M(N+1) origin/dev-MN
   ```
   On exit 0, push the fast-forward and stop. No PR is necessary.
2. Otherwise merge with `--no-ff` and resolve by hand, per the rules above.
3. After resolving, confirm nothing was dropped:
   ```
   git diff origin/dev-MN...HEAD    -- <file>
   git diff origin/dev-M(N+1)...HEAD -- <file>
   ```
   Neither diff should show a lost change, such as a dependency tag that went backwards.
4. Record every conflict in the PR body: what each side changed, which side you kept, and why. A
   reviewer must be able to check your decision without re-running the merge. `git show --cc HEAD --
   <file>` prints only the hunks where the result differs from both parents.
5. List follow-up commits on the sync branch by short SHA and purpose.

When there were no conflicts and no follow-up commits, the body is `No conflicts.`

## Manual MQTT testing

The MQTTX CLI (`mqttx`) is the first choice for manual MQTT tests. It is also a good default for
the reproduction steps in an issue or a PR, because most people here have it installed. Use a
different client when it suits the task better.

```
mqttx pub -h <host> -u <user> -q 1 -t t/1 -m hello
mqttx sub -h <host> -u <user> -q 1 -t 't/#'
```

Two `mqttx` behaviours that have produced wrong conclusions before:

- `mqttx pub` prints `Message published` even when the broker **rejected** the publish. Check the
  output for an `Error:` line, for example `Error: Publish error: Quota exceeded`.
- `mqttx sub` runs until stopped, so wrap it in `timeout` inside a script. A rejected subscription
  prints `Subscription negated to <topic> with code <N>` rather than failing.

`mqttx` speaks MQTT 5.0 by default; pass `-V 3.1.1` or `-V 3.1` for an older version.

## Running someone else's reproduction

Read the script. Run it only against a node that you started. A reproduction attached to an issue
can send malformed input and can suspend an internal process, which affects every session on the
same shard. Write your own version of the script when you can.

## Shared machines

Several agents may share one build host.

- `emqx_cth_cluster` uses the fixed node name `test@127.0.0.1` and `base_port(N) = 10000 + N*100`.
  A collision makes cases auto-skip while the suite still reports `0 failed`. Check `epmd -names`
  and listening ports before a run, and report what they showed.
- Check free memory and system load before a benchmark. A benchmark that runs at the same time as
  a CT suite gives incorrect numbers. Report this instead of publishing the numbers.

## Writing

Follow the writing rules in CONTRIBUTING.md. They are a preference for a human contributor. For an
agent they are a requirement.

Two points that apply to agent-authored text:

- A code comment says what the code does. The commit message says **why it changed** — the defect,
  the alternative that you rejected, the history of the line. A comment that explains a past
  mistake becomes incorrect when the code moves.
- Do not repeat the diff in the PR body. Write what the reviewer cannot see — the cause, the
  decision that you made, and what you verified.

### Read your own pull request

After you push, read the description and the diff again, as a reviewer who has not seen the issue.
A reviewer must not have to work to find what changed and why. A long description costs more to
read than a short one, so delete every line that does not help the decision.

A line that only repeats one of these does not help:

- the issue description, which the reviewer can open;
- what the code says, which the reviewer is reading.

Some detail is written for an agent rather than for a person: test output, a long trace, the
searches you ran, the branches you checked. Fold it into a `<details>` block with a `<summary>`
that says what it holds. A reviewer then skips it in one movement, and the next agent still has
it.

```markdown
<details><summary>Test output</summary>

    emqx_frame_SUITE: 42 ok, 0 failed, 0 skipped

</details>
```

Leave a block out when it only repeats the issue or the code. A collapsed section is cheap to
skip, not free.

### Do not contaminate another reviewer's context

Another agent may review your work, and an agent treats a confident sentence as settled. A claim
written as a fact stops the next reader from checking it, so an incorrect claim survives.

Write a finding as a finding. State what you observed and how you observed it, and keep the
conclusion separate from the evidence:

- "The suite passes on this branch" when you ran it, not "this change is safe".
- "I found no other caller; I searched `apps/` and `deps/` for `foo/2`", not "there are no other
  callers".
- Name what you did not check. A reader assumes you checked everything that you do not exclude.

This applies to a code comment as well. A comment that asserts an invariant nobody verified is
worse than no comment, because the next change is built on it.
