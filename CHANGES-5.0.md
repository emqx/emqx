# 5.0.7

## Bug fixes

* Remove the needless `will_msg` field from the client API. [#8721](https://github.com/emqx/emqx/pull/8721)
* Fix `$queue` topic name error in management API return. [#8728](https://github.com/emqx/emqx/pull/8728)
* Fix sometimes `client.connected` and `client.disconnected` could be in wrong order. [#8625](https://github.com/emqx/emqx/pull/8625)

# 5.0.6

## Bug fixes

* Upgrade Dashboard version to fix an issue where the node status was not displayed correctly. [#8771](https://github.com/emqx/emqx/pull/8771)

# 5.0.5

## Bug fixes

* Allow changing the license type from key to file (and vice-versa). [#8598](https://github.com/emqx/emqx/pull/8598)
* Add back http connector config keys `max_retries` `retry_interval` as deprecated fields [#8672](https://github.com/emqx/emqx/issues/8672)
  This caused upgrade failure in 5.0.4, because it would fail to boot on configs created from older version.

## Enhancements

* The license is now copied to all nodes in the cluster when it's reloaded. [#8598](https://github.com/emqx/emqx/pull/8598)
* Added a HTTP API to manage licenses. [#8610](https://github.com/emqx/emqx/pull/8610)
* Updated `/nodes` API node_status from `Running/Stopped` to `running/stopped`. [#8642](https://github.com/emqx/emqx/pull/8642)
* Improve handling of placeholder interpolation errors [#8635](https://github.com/emqx/emqx/pull/8635)
* Better logging on unknown object IDs. [#8670](https://github.com/emqx/emqx/pull/8670)
* The bind option support `:1883` style. [#8758](https://github.com/emqx/emqx/pull/8758)

# 5.0.4

## Bug fixes

* The `data/configs/cluster-override.conf` is cleared to 0KB if `hocon_pp:do/2` failed [commits/71f64251](https://github.com/emqx/emqx/pull/8443/commits/71f642518a683cc91a32fd542aafaac6ef915720)
* Improve the health_check for webhooks.
  Prior to this change, the webhook only checks the connectivity of the TCP port using `gen_tcp:connect/2`, so
  if it's a HTTPs server, we didn't check if TLS handshake was successful.
  [commits/6b45d2ea](https://github.com/emqx/emqx/commit/6b45d2ea9fde6d3b4a5b007f7a8c5a1c573d141e)
* The `created_at` field of rules is missing after emqx restarts. [commits/5fc09e6b](https://github.com/emqx/emqx/commit/5fc09e6b950c340243d7be627a0ce1700691221c)
* The rule engine's jq function now works even when the path to the EMQX install dir contains spaces [jq#35](https://github.com/emqx/jq/pull/35) [#8455](https://github.com/emqx/emqx/pull/8455)
* Avoid applying any ACL checks on superusers [#8452](https://github.com/emqx/emqx/pull/8452)
* Fix statistics related system topic name error
* Fix AuthN JWKS SSL schema. Using schema in `emqx_schema`. [#8458](https://github.com/emqx/emqx/pull/8458)
* `sentinel` field should be required when AuthN/AuthZ Redis using sentinel mode. [#8458](https://github.com/emqx/emqx/pull/8458)
* Fix bad swagger format. [#8517](https://github.com/emqx/emqx/pull/8517)
* Fix `chars_limit` is not working when `formatter` is `json`. [#8518](http://github.com/emqx/emqx/pull/8518)
* Ensuring that exhook dispatches the client events are sequential. [#8530](https://github.com/emqx/emqx/pull/8530)
* Avoid using RocksDB backend for persistent sessions when such backend is unavailable. [#8528](https://github.com/emqx/emqx/pull/8528)
* Fix AuthN `cert_subject` and `cert_common_name` placeholder rendering failure. [#8531](https://github.com/emqx/emqx/pull/8531)
* Support listen on an IPv6 address, e.g: [::1]:1883 or ::1:1883. [#8547](https://github.com/emqx/emqx/pull/8547)
* GET '/rules' support for pagination and fuzzy search. [#8472](https://github.com/emqx/emqx/pull/8472)
  **‼️ Note** : The previous API only returns array: `[RuleObj1,RuleObj2]`, after updating, it will become
  `{"data": [RuleObj1,RuleObj2], "meta":{"count":2, "limit":100, "page":1}`,
  which will carry the paging meta information.
* Fix the issue that webhook leaks TCP connections. [ehttpc#34](https://github.com/emqx/ehttpc/pull/34), [#8580](https://github.com/emqx/emqx/pull/8580)

## Enhancements

* Improve the dashboard listener startup log, the listener name is no longer spliced with port information,
  and the colon(:) is no longer displayed when IP is not specified. [#8480](https://github.com/emqx/emqx/pull/8480)
* Remove `/configs/listeners` API, use `/listeners/` instead. [#8485](https://github.com/emqx/emqx/pull/8485)
* Optimize performance of builtin database operations in processes with long message queue [#8439](https://github.com/emqx/emqx/pull/8439)
* Improve authentication tracing. [#8554](https://github.com/emqx/emqx/pull/8554)
* Standardize the '/listeners' and `/gateway/<name>/listeners` API fields.
  It will introduce some incompatible updates, see [#8571](https://github.com/emqx/emqx/pull/8571)
* Add option to perform GC on connection process after TLS/SSL handshake is performed. [#8637](https://github.com/emqx/emqx/pull/8637)

# 5.0.3

## Bug fixes

* Websocket listener failed to read headers `X-Forwarded-For` and `X-Forwarded-Port` [#8415](https://github.com/emqx/emqx/pull/8415)
* Deleted `cluster_singleton` from MQTT bridge config document. This config is no longer applicable in 5.0 [#8407](https://github.com/emqx/emqx/pull/8407)
* Fix `emqx/emqx:latest` docker image publish to use the Erlang flavor, but not Elixir flavor [#8414](https://github.com/emqx/emqx/pull/8414)
* Changed the `exp` field in JWT auth to be optional rather than required to fix backwards compatability with 4.X releases. [#8425](https://github.com/emqx/emqx/pull/8425)

## Enhancements

* Improve the speed of dashboard's HTTP API routing rule generation, which sometimes causes timeout [#8438](https://github.com/emqx/emqx/pull/8438)

# 5.0.2

Announcement: EMQX team has decided to stop supporting relup for opensource edition.
Going forward, it will be an enterprise-only feature.

Main reason: relup requires carefully crafted upgrade instructions from ALL previous versions.

For example, 4.3 is now at 4.3.16, we have `4.3.0->4.3.16`, `4.3.1->4.3.16`, ... 16 such upgrade paths in total to maintain.
This had been the biggest obstacle for EMQX team to act agile enough in delivering enhancements and fixes.

## Enhancements

## Bug fixes

* Fixed a typo in `bin/emqx` which affects MacOs release when trying to enable Erlang distribution over TLS [#8398](https://github.com/emqx/emqx/pull/8398)
* Restricted shell was accidentally disabled in 5.0.1, it has been added back. [#8396](https://github.com/emqx/emqx/pull/8396)

# 5.0.1

5.0.1 is built on [Erlang/OTP 24.2.1-1](https://github.com/emqx/otp/tree/OTP-24.2.1-1). Same as 5.0.0.

5.0.0 (like 4.4.x) had Erlang/OTP version number in the package name.
This is because we wanted to release different flavor packages (on different Elixir/Erlang/OTP platforms).

However the long package names also causes confusion, as users may not know which to choose if there were more than
one presented at the same time.

Going forward, (starting from 5.0.1), packages will be released in both default (short) and flavored (long) package names.

For example: `emqx-5.0.1-otp24.2.1-1-ubuntu20.04-amd64.tar.gz`,
but only the default one is presented to the users: `emqx-5.0.1-ubuntu20.04-amd64.tar.gz`.

In case anyone wants to try a different flavor package, it can be downlowded from the public s3 bucket,
for example:
https://s3.us-west-2.amazonaws.com/packages.emqx/emqx-ce/v5.0.1/emqx-5.0.1-otp24.2.1-1-ubuntu20.04-arm64.tar.gz

Exceptions:

* Windows package is always presented with short name (currently on Erlang/OTP 24.2.1).
* Elixir package name is flavored with both Elixir and Erlang/OTP version numbers,
  for example: `emqx-5.0.1-elixir1.13.4-otp24.2.1-1-ubuntu20.04-amd64.tar.gz`

## Enhancements

* Removed management API auth for prometheus scraping endpoint /api/v5/prometheus/stats [#8299](https://github.com/emqx/emqx/pull/8299)
* Added more TCP options for exhook (gRPC) connections. [#8317](https://github.com/emqx/emqx/pull/8317)
* HTTP Servers used for authentication and authorization will now indicate the result via the response body. [#8374](https://github.com/emqx/emqx/pull/8374) [#8377](https://github.com/emqx/emqx/pull/8377)
* Bulk subscribe/unsubscribe APIs [#8356](https://github.com/emqx/emqx/pull/8356)
* Added exclusive subscription [#8315](https://github.com/emqx/emqx/pull/8315)
* Provide authentication counter metrics [#8352](https://github.com/emqx/emqx/pull/8352) [#8375](https://github.com/emqx/emqx/pull/8375)
* Do not allow admin user self-deletion [#8286](https://github.com/emqx/emqx/pull/8286)
* After restart, ensure to copy `cluster-override.conf` from the clustered node which has the greatest `tnxid`. [#8333](https://github.com/emqx/emqx/pull/8333)

## Bug fixes

* A bug fix ported from 4.x: allow deleting subscriptions from `client.subscribe` hookpoint callback result. [#8304](https://github.com/emqx/emqx/pull/8304) [#8347](https://github.com/emqx/emqx/pull/8377)
* Fixed Erlang distribution over TLS [#8309](https://github.com/emqx/emqx/pull/8309)
* Made possible to override authentication configs from environment variables [#8323](https://github.com/emqx/emqx/pull/8309)
* Made authentication passwords in Mnesia database backward compatible to 4.x, so we can support data migration better. [#8351](https://github.com/emqx/emqx/pull/8351)
* Fix plugins upload for rpm/deb installations [#8379](https://github.com/emqx/emqx/pull/8379)
* Sync data/authz/acl.conf and data/certs from clustered nodes after a new node joins the cluster [#8369](https://github.com/emqx/emqx/pull/8369)
* Ensure auto-retry of failed resources [#8371](https://github.com/emqx/emqx/pull/8371)
* Fix the issue that the count of `packets.connack.auth_error` is inaccurate when the client uses a protocol version below MQTT v5.0 to access [#8178](https://github.com/emqx/emqx/pull/8178)

## Others

* Rate limiter interface is hidden so far, it's subject to a UX redesign.
* QUIC library upgraded to 0.0.14.
* Now the default packages will be released withot otp version number in the package name.
* Renamed config exmpale file name in `etc` dir.
