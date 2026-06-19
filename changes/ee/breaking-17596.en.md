Added authorization options that forbid interpolation of `/`, `+`, and `#` symbols into topic filter templates in authorization rules. The new options are:
```
authorization.topic_template_allow {
    plus => false,
    hash => false,
    slash => false
}
```
With `false`, the corresponding symbol cannot be used in a value interpolated into a topic template.
For example, if `plus = false`, then username `bad+user` is forbidden in a rule such as `{allow, all, publish, ["userspace/${username}"]}`. The outcome depends on the active security profile: with the legacy profile the rule will not match, and with the hardened profile the action will be denied.
