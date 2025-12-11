The internal regex engine has been upgraded to PCRE2.
This provides faster matching but enforces stricter syntax rules.

If you use `regex_match`, `regex_replace` or `regex_extract` functions in your SQL rules,
existing patterns with "sloppy" syntax (like invalid escape sequences) that worked by accident may now fail.

Key changes to watch for:

- Stricter Escaping: Invalid escape sequences that were previously ignored are now treated as errors.
- Broken: [\w-\.] (Escaping a dot inside [] is often unnecessary but previously allowed; strictly, only special chars should be escaped).
- Broken: \x without following hex digits (e.g., \xGG) will now cause a compile error instead of being treated as a literal "x".
- Stricter Group Names: Duplicate group names or empty group names in patterns are no longer allowed.

Action Required: Audit your Rule Engine SQL definitions. If you use complex regular expressions, verify them against a PCRE2-compliant tester (most online regex testers support PCRE2) or test them in a staging environment before upgrading.
