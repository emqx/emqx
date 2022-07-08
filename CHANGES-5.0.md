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

# 5.0.3

## Bug fixes

* Websocket listener failed to read headers `X-Forwarded-For` and `X-Forwarded-Port` [#8415](https://github.com/emqx/emqx/pull/8415)
* Deleted `cluster_singleton` from MQTT bridge config document. This config is no longer applicable in 5.0 [#8407](https://github.com/emqx/emqx/pull/8407)
* Fix `emqx/emqx:latest` docker image publish to use the Erlang flavor, but not Elixir flavor [#8414](https://github.com/emqx/emqx/pull/8414)
* Changed the `exp` field in JWT auth to be optional rather than required to fix backwards compatability with 4.X releases. [#8425](https://github.com/emqx/emqx/pull/8425)

## Enhancements

* Improve the speed of dashboard's HTTP API routing rule generation, which sometimes causes timeout [#8438](https://github.com/emqx/emqx/pull/8438)

# 5.0.2

Announcemnet: EMQX team has decided to stop supporting relup for opensouce edition.
Going forward, it will be an enterprise only feature.

Main reason: relup requires carefully crafted upgrade instructions from ALL previous versions.

For example, 4.3 is now at 4.3.16, we have `4.3.0->4.3.16`, `4.3.1->4.3.16`, ... 16 such upgrade paths in total to maintain.
This had been the biggest obstacle for EMQX team to act agile enough in deliverying enhancements and fixes.

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
