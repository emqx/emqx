# EMQX Sparkplug Awareness Plugin

Makes EMQX Sparkplug 3.0-aware, in the sense defined in the [Sparkplug 3.0 specification](https://sparkplug.eclipse.org/specification/version/3.0/documents/sparkplug-specification-3.0.0.pdf), section 10.1.4.  Namely, by having this plugin enabled, EMQX will track all `NBIRTH` and `DBIRTH` messages it sees and serve their latest seen version under the special `$sparkplug/certificates/` prefix as retained messages.  Currently, `NDEATH` message timestamps are not replaced.
