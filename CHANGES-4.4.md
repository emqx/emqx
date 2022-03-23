# EMQ X 4.4 Changes

## v4.4.2

**NOTE**: v4.4.2 is in sync with: v4.3.13

### Important changes

* Docker image is based on alpine-3.15.1 (OpenSSL-1.1.1n)
* For docker image, /opt/emqx/etc has been removed from the VOLUME list,
  this made it easier for the users to rebuild image on top with changed configs.

### Enhancements

* Windows package is built on Erlang/OTP 24

### Enhancements (synced from v4.3.13)

* CLI `emqx_ctl pem_cache clean` to force purge x509 certificate cache,
  to force an immediate reload of all certificates after the files are updated on disk.
* Refactor the ExProto so that anonymous clients can also be displayed on the dashboard [#6983]
* Force shutdown of processes that cannot answer takeover event [#7026]
* Support set keepalive via queryString & Body HTTP API.
* `topic` parameter in bridge configuration can have `${node}` substitution (just like in `clientid` parameter)
* Add UTF-8 string validity check in `strict_mode` for MQTT packet.
  When set to true, invalid UTF-8 strings will cause the client to be disconnected. i.e. client ID, topic name. [#7261]
* Changed systemd service restart delay from 10 seconds to 60 seconds.
* MQTT-SN gateway supports initiative to synchronize registered topics after session resumed. [#7300]
* Add load control app for future development.
* Change the precision of float to 17 digits after the decimal point when formatting a
  float using payload templates of rule actions. The old precision is 10 digits before
  this change.

### Bug fixes (synced from v4.3.13)

* Fix the `{error,eexist}` error when do release upgrade again if last run failed. [#7121]
* Fix case where publishing to a non-existent topic alias would crash the connection [#6979]
* Fix HTTP-API 500 error on querying the lwm2m client list on the another node [#7009]
* Fix the ExProto connection registry is not released after the client process abnormally exits [#6983]
* Fix Server-KeepAlive wrongly applied on MQTT v3.0/v3.1 [#7085]
* Fix Stomp client can not trigger `$event/client_connection` message [#7096]
* Fix system memory false alarm at boot
* Fix the MQTT-SN message replay when the topic is not registered to the client [#6970]
* Fix rpc get node info maybe crash when other nodes is not ready.
* Fix false alert level log “cannot_find_plugins” caused by duplicate plugin names in `loaded_plugins` files.
* Prompt user how to change the dashboard's initial default password when emqx start.
* Fix errno=13 'Permission denied' Cannot create FIFO boot error in Amazon Linux 2022 (el8 package)
* Fix user or appid created, name only allow `^[A-Za-z]+[A-Za-z0-9-_]*$`
* Fix subscribe http api crash by bad_qos `/mqtt/subscribe`,`/mqtt/subscribe_batch`.
* Send DISCONNECT packet with reason code 0x98 if connection has been kicked [#7309]
* Fix make all traces stopped when emqx_trace_module is disabled.

## v4.4.1

This patch release is only to fix windows build which failed on v4.4.0.

## v4.4.0

**NOTE**: v4.4.0 is in sync with: v4.3.12

### Important changes

- **For Debian/Ubuntu users**, Debian/Ubuntu package (deb) installed EMQ X is now started from systemd.
  This is to use systemd's supervision functionality to ensure that EMQ X service restarts after a crash.
  The package installation service upgrade from init.d to systemd has been verified,
  it is still recommended that you verify and confirm again before deploying to the production environment,
  at least to ensure that systemd is available in your system

- Package name scheme changed comparing to 4.3.
  4.3 format: emqx-centos8-4.3.8-amd64.zip
  4.4 format: emqx-4.4.0-rc.1-otp24.1.5-3-el8-amd64.zip
  * Erlang/OTP version is included in the package name,
    providing the possibility to release EMQX on multiple Erlang/OTP versions
  * `centos` is renamed to `el`. This is mainly due to centos8 being dead (replaced with rockylinux8)

- MongoDB authentication supports DNS SRV and TXT Records resolution, which can seamlessly connect with MongoDB Altas

- Support dynamic modification of MQTT Keep Alive to adapt to different energy consumption strategies.

- Support 4.3 to 4.4 rolling upgrade of clustered nodes. See upgrade document for more dtails.

- TLS for cluster backplane (RPC) connections. See clustering document for more details.

- Support real-time tracing in the dashboard, with Client ID, Client IP address, and topic name based filtering.

- Add the Slow Subscriptions module to count the time spent during the message transmission. This feature will list the Clients and Topics with higher time consumption in Dashboard

### Minor changes

- Bumpped default boot wait time from 15 seconds to 150 seconds
  because in some simulated environments it may take up to 70 seconds to boot in build CI

- Dashboard supports relative paths and custom access paths

- Supports configuring whether to forward retained messages with empty payload to suit users
  who are still using MQTT v3.1. The relevant configurable item is `retainer.stop_publish_clear_msg`

- Multi-language hook extension (ExHook) supports dynamic cancellation of subsequent forwarding of client messages

- Rule engine SQL supports the use of single quotes in `FROM` clauses, for example: `SELECT * FROM 't/#'`

- Change the default value of the `max_topic_levels` configurable item to 128.
  Previously, it had no limit (configured to 0), which may be a potential DoS threat

- Improve the error log content when the Proxy Protocol message is received without `proxy_protocol` configured.

- Add additional message attributes to the message reported by the gateway.
  Messages from gateways such as CoAP, LwM2M, Stomp, ExProto, etc., when converted to EMQ X messages,
  add fields such as protocol name, protocol version, user name, client IP, etc.,
  which can be used for multi-language hook extension (ExHook)

- HTTP client performance improvement

- Add openssl-1.1 to RPM dependency
