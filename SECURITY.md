# Security Policy

## Supported Versions

Security fixes are produced for the versions below. LTS lines receive fixes for
their full support window; other lines receive fixes until the next minor
release supersedes them.

| Version | Status | Supported |
| --- | --- | --- |
| 7.0 | Alpha, not for production | :white_check_mark: |
| 6.3 | LTS | :white_check_mark: |
| 6.2 | | :white_check_mark: |
| 6.1 | | :white_check_mark: |
| 6.0 | | :white_check_mark: |
| 5.10 | | :white_check_mark: |
| 5.8 | LTS | :white_check_mark: |
| Any earlier release | End of life | :x: |

7.0 is in alpha. We accept and fix security reports against it, but it must not
be used in production and findings that exist only in unreleased 7.0 code are
handled on a best-effort schedule rather than through the release process below.

When reporting, state the exact version you tested. "Latest" is ambiguous: give
the release number, and for container images the image digest.

### Backports are not automatic

A supported version receives security fixes, but not every fix is backported to
every supported line. A fix may land only on the newer lines, for example when
it depends on a mechanism that does not exist on an older line, or when
applying it to a maintenance release would be a breaking change. The 5.x and
6.x lines do not share fixes with each other.

Before reporting, check the current development branch — **`dev-70`** — which
carries the newest code. If the behaviour is already fixed there, it is not a
new report; whether and when it reaches an older supported line is a backport
decision we track internally.

The branch to check changes as development moves on. Use the highest-numbered
`dev-*` branch in this repository.

## Reporting a Vulnerability

Send reports to **security@emqx.io**. Do not open a public GitHub issue, pull
request or discussion for an unreported vulnerability.

Include:

1. The affected version, and the image digest if you used a container.
2. The configuration that makes it reachable, including anything you changed
   from the shipped defaults.
3. Reproduction steps in plain text. A video or screenshots alone are not
   enough; we need the steps and the observed output in text we can replay.
4. The observed result and the result you expected.
5. Where you can, a control that shows the boundary works when the flaw is not
   triggered. A report that shows both the bypass and the matching denial is
   much faster to confirm.

Please also:

- Test only against instances you operate. Never access anyone else's data, and
  do not run anything disruptive against systems you do not own.
- Send one report per root cause. If one root cause affects several products or
  endpoints, keep it in one report rather than splitting it.
- Send separate reports for separate root causes, rather than bundling unrelated
  findings.

We acknowledge receipt, tell you whether we consider the report in scope, and
keep you informed while we work. We will tell you when a fix is released and in
which versions. Disclosure terms are below and apply to every accepted report.

## Coordinated Disclosure

These terms apply to reports we accept.

**Hold publication until 90 days after the fix ships on the latest stable
release line.** That line is currently 6.3; while 7.0 is in alpha it does not
start the clock. A fix often reaches several supported lines at different times,
so the 90 days run from the release on the latest stable line, not from the
first line to receive the fix, and not from the date we confirm the report.

We will tell you the release that starts the clock and the date it ends. If a
fix is taking longer than you expected, raise it with us rather than publishing
early; we would rather extend the conversation than have a fix and a write-up
land out of order.

**We publish through GitHub Security Advisories on this repository**, at
https://github.com/emqx/emqx/security/advisories, and we would like to do that
with you rather than around you. For an accepted report we open a draft
advisory, add you as a collaborator on it so you can read and comment on the
text before it goes out, and credit you unless you ask us not to.

We do not always request a CVE identifier. Whether to request one is our
decision, taken per report, and an advisory without a CVE is not a statement
that the finding was unimportant. If you believe a report warrants one, say so
in the draft and we will consider it.

If you intend to publish your own write-up, tell us the date. We will aim to
publish the advisory at the same time so the two agree on the facts and on the
affected versions. If your write-up and our advisory disagree about scope, say
so before publication and we will resolve it in the draft.

Reports we do not accept, including those matching the out-of-scope categories
below and those matching an issue we already track internally, carry no
disclosure restriction from us. We would still appreciate knowing when you plan
to publish.

## In Scope

Any design or implementation issue that substantially affects the
confidentiality, integrity or availability of user data or of the broker.
Examples:

- Authentication and authorization flaws, including privilege escalation and
  cross-tenant access.
- Server-side code execution.
- Disclosure of credentials, keys or message payloads to a party not entitled
  to them, including through logs, API responses, support bundles and audit
  records.
- Protocol parsing and resource-exhaustion defects reachable before
  authentication.
- Cross-site scripting, cross-site request forgery and mixed content in the
  Dashboard.

## Out of Scope

Reports in the categories below are closed as known or non-qualifying. We would
rather say this plainly than leave reporters to spend effort on work we cannot
act on. Whether we later change a documented default or design is a separate
engineering decision and is not affected by a report.

### 1. Behaviour already documented

If the behaviour is described in the EMQX documentation, it is a documented
design decision or a documented hardening step, not a vulnerability. Please
check the documentation before reporting. In particular:

- The [security checklist](https://docs.emqx.com/en/emqx/latest/guides/access-control/security-checklist.html),
  which covers listener exposure, restricting inter-node ports, replacing the
  default node cookie, changing the default Dashboard password, setting
  `authorization.no_match = deny`, forwarded-header validation, TLS, file
  permissions for secrets and API key use.
- The **Security considerations** section on a feature's own documentation page.
  For example, Message Queue and MQTT Streams document that a subscription is
  authorized against the whole subscription topic filter, including the
  `$queue/` or `$stream/` prefix and the queue or stream name, and not against
  the queue's or stream's capture filter.
- The multi-tenancy documentation, which states that topic isolation between
  namespaces is not automatic and requires a mountpoint.

### 2. Issues already public

If the issue is already being discussed publicly — a GitHub issue, discussion
or pull request, a published advisory, or a public write-up — it is not a new
report. Please link the existing discussion instead.

### 3. Other non-qualifying reports

- Findings that require a configuration no reasonable deployment would use, or
  that assume the attacker already holds broker administrator access or host
  access, where the report shows no boundary being crossed.
- Missing hardening with no demonstrated impact, including scanner output and
  best-practice checklists submitted without a working reproduction.
- Vulnerabilities in a dependency with no demonstrated reachable path in EMQX.
  A CVE identifier for a library we ship is not by itself a report; show how it
  is reachable.
- User enumeration, and brute force against authentication.
- Reports about end-of-life versions, unless the same defect is present in a
  supported version — in which case report it against the supported version.
- Defects with no security consequence. Please file those as ordinary GitHub
  issues; they are welcome there.

## Known Issues That Are Not Public

We track security work in a private repository.
Some known issues are recorded only there, so they are not visible in the
documentation, in GitHub issues, or in any advisory. You cannot be expected to
know about them before reporting.

We are open to reports that turn out to match one of these. We will tell you that
the issue is already tracked and give you the internal reference
(`emqx-dev-team-tasks#NUM`) so you know it is a real record, even though the
repository is private and you will not be able to open it.

Such a report is **not treated as a new finding**. We will still confirm the
technical details with you, and we will correct any part of your analysis that
does not match our own, because that is useful to both sides.

This is different from the out-of-scope categories above: those are closed, while
an internally tracked issue is open work that your report does not change the
status of.

## Testing Conduct

- Do not run denial-of-service or load tests against anything you do not own,
  and do not use EMQ-operated systems as a target.
- Avoid automated scanners and fuzzers that generate large traffic volumes
  against shared infrastructure. Against your own instance, use whatever you
  like.
- Do not attempt social engineering, phishing, or physical access.
