
# EMQX Management

EMQX Management offers various interfaces for administrators to interact with
the system, either by a remote console attached to a running node, a CLI (i.e.
`./emqx ctl`), or through its rich CRUD-style REST API (mostly used by EMQX'
dashboard). The system enables administrators to modify both cluster and
individual node configurations, and provides the ability to view and reset
different statistics and metrics.

## Functionality

Amongst others it allows to manage

* alarms,
* API Keys,
* banned clients, users or hosts,
* connected clients including their topic subscriptions,
* cluster configurations,
* configuration of MQTT listeners,
* node configuration,
* custom plugins,
* fixed subscriptions,
* and topics.

Moreover it lets you

* modify hot and non-hot updatable configuration values,
* publish messages, as well as bulk messages,
* create trace files,
* and last but not least monitor system status.

## Implementation Notes

API endpoints are implemented using the `minirest` framework in combination with
`hoconsc` and `emqx_dashboard_swagger`.

## TODO/FIXME

At its current state there are some reverse dependencies from other applications
that do calls directly into `emqx_mgmt`.

Also, and somewhat related, its bpapi proto modules do calls directly into
other applications.
