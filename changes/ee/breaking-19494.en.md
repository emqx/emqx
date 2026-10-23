`emqx ctl conf show` now redacts sensitive values by default, for the full config, a single root key
and a namespaced config alike. The new `--no-secret-redaction` flag prints the stored values
instead; an export of a single writable root can be loaded back with `conf load`, while a raw full
export is meant to be used as `etc/emqx.conf` when restarting a node. Such exports do not include
hidden roots or the contents of `file://` references, and should be stored as secrets.
