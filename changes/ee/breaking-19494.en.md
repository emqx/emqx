`emqx ctl conf show` now redacts sensitive values by default, for the full config, a single root key
and a namespaced config alike. The new `--no-secret-redaction` flag prints the stored values
instead; scripts that export a configuration to load it back later should use it. Such exports do
not include hidden roots or the contents of `file://` references, and should be stored as secrets.
