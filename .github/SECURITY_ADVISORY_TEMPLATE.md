# EMQX Security Advisory template

This file mirrors the GitHub advisory page's description template. New advisories should follow the structure below; the **Metadata** section maps to fields in the GitHub web UI rather than free-form text.


## Description template

### Impact
_What kind of vulnerability is it? Who is impacted?_

### Patches
_Has the problem been patched? What versions should users upgrade to?_

### Workarounds
_Is there a way for users to fix or remediate the vulnerability without upgrading?_

### References
_Are there any links users can visit to find out more?_


## Metadata

The fields below correspond to GitHub advisory UI inputs, not free-form text.

### Affected products

Affected product name: `EMQX Enterprise <Major.Minor>` — one entry per affected minor.

For each entry, the **Affected versions** range follows the convention:

- Lower bound: `>=` the first version that contains the vulnerability.
- Upper bound: `<` the first version that contains the fix.

Example: `>= 5.8.1, < 5.8.11` (comma means AND) — the vulnerability was introduced in 5.8.1 and 5.8.11 is the first version that includes the fix.

If the vulnerability has been present since the first version of the minor (e.g. 5.8.0), omit the `>=` lower bound and write only the upper bound (e.g. `< 5.8.11`).

### Patched versions

The first bug-fix release on each affected minor that includes the fix.

NOTE: the advisory is published only after the patched versions have been released, so the advisory uses past tense ("has been patched", "was introduced in").

### Severity

Fill in the CVSS vector and a short reason for each metric (AV, AC, PR, UI, S, C, I, A).

### Common weakness enumerator (CWE)

Recommend CWE entries to add. List the primary CWE first and any secondary CWEs after, each with a one-line justification.
