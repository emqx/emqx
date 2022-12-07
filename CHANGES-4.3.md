# EMQX 4.3 Changes

Started tracking changes in CHANGE.md since EMQX v4.3.11

NOTE: Keep prepending to the head of the file instead of the tail

File format:

- Use weight-2 heading for releases
- One list item per change topic
  Change log ends with a list of GitHub PRs

## For 4.3.22 and later versions, please find details in `changes` dir

## v4.3.21

### Bug fixes

- Deny POST an existing resource id using HTTP API with error 400 "Already Exists". [#9079](https://github.com/emqx/emqx/pull/9079)
- Fix the issue that reseting rule metrics crashed under certain conditions. [#9079](https://github.com/emqx/emqx/pull/9079)

### Enhancements

- TLS listener memory usage optimization [#9005](https://github.com/emqx/emqx/pull/9005).
  New config `listener.ssl.$NAME.hibernate_after` to hibernate TLS connection process after idling.
  Hibernation can reduce RAM usage significantly, but may cost more CPU.
  This configuration is by default disabled.
  Our preliminary test shows a 50% of RAM usage decline when configured to '5s'.

- TLS listener default buffer size to 4KB [#9007](https://github.com/emqx/emqx/pull/9007)
  Eliminate uncertainty that the buffer size is set by OS default.

- Disable authorization for `api/v4/emqx_prometheus` endpoint. [#8955](https://github.com/emqx/emqx/pull/8955)

- Added a test to prevent a last will testament message to be
  published when a client is denied connection. [#8894](https://github.com/emqx/emqx/pull/8894)

- More rigorous checking of flapping to improve stability of the system. [#9045](https://github.com/emqx/emqx/pull/9045)

- QoS1 and QoS2 messages in session's buffer are re-dispatched to other members in the group
  when the session terminates [#9094](https://github.com/emqx/emqx/pull/9094).
  Prior to this enhancement, one would have to set `broker.shared_dispatch_ack_enabled` to true
  to prevent sessions from buffering messages, however this acknowledgement comes with a cost.

- Prior to this fix, some of the time stamps were taken from the `os` module (system call),
  while majority of other places are using `erlang` module (from Erlang virtual machine).
  This inconsistent behaviour has caused some trouble for the Delayed Publish feature when OS time changes.
  Now all time stamps are from `erlang` module. [#8908](https://github.com/emqx/emqx/pull/8908)

### Bug fixes

- Fix HTTP client library to handle SSL socket passive signal. [#9145](https://github.com/emqx/emqx/pull/9145)

- Hide redis password in error logs [#9071](https://github.com/emqx/emqx/pull/9071)
  More changes in redis client included in this release:
  - Improve redis connection error logging [eredis #19](https://github.com/emqx/eredis/pull/19).
    Also added support for eredis to accept an anonymous function as password instead of
    passing around plaintext args which may get dumpped to crash logs (hard to predict where).
    This change also added `format_status` callback for `gen_server` states which hold plaintext
    password so the process termination log and `sys:get_status` will print '******' instead of
    the password to console.
  - Avoid pool name clashing [eredis_cluster #22](https://github.com/emqx/eredis_cluster/pull/22)
    Same `format_status` callback is added here too for `gen_server`s which hold password in
    their state.

- Fix shared subscription message re-dispatches [#9094](https://github.com/emqx/emqx/pull/9094).
  - When discarding QoS 2 inflight messages, there were excessive logs
  - For wildcard deliveries, the re-dispatch used the wrong topic (the publishing topic,
    but not the subscribing topic), caused messages to be lost when dispatching.

- Fix shared subscription group member unsubscribe issue when 'sticky' strategy is used.
  Prior to this fix, if a previously picked member unsubscribes from the group (without reconnect)
  the message is still dispatched to it.
  This issue only occurs when unsubscribe with the session kept.
  Fixed in [#9119](https://github.com/emqx/emqx/pull/9119)

- Fix shared subscription 'sticky' strategy when there is no local subscriptions at all.
  Prior to this change, it may take a few rounds to randomly pick group members until a local subscriber
  is hit (and then start sticking to it).
  After this fix, it will start sticking to whichever randomly picked member even when it is a
  subscriber from another node in the cluster.
  Fixed in [#9122](https://github.com/emqx/emqx/pull/9122)

- Fix rule engine fallback actions metrics reset. [#9125](https://github.com/emqx/emqx/pull/9125)

## v4.3.20

### Bug fixes

- Fix rule-engine update behaviour which may initialize actions for disabled rules. [#8849](https://github.com/emqx/emqx/pull/8849)
- Fix JWT plugin don't support non-integer timestamp claims. [#8862](https://github.com/emqx/emqx/pull/8862)
- Fix a possible dead loop caused by shared subscriptions with `shared_dispatch_ack_enabled=true`. [#8918](https://github.com/emqx/emqx/pull/8918)
- Fix dashboard binding IP address not working. [#8916](https://github.com/emqx/emqx/pull/8916)
- Fix rule SQL topic matching to null values failed. [#8927](https://github.com/emqx/emqx/pull/8927)
  The following SQL should not fail (crash) but return `{"r": false}`:
  `SELECT topic =~ 't' as r FROM "$events/client_connected"`.
  The topic is a null value as there's no such field in event `$events/client_connected`, so it
  should return false if match it to a topic.

## v4.3.19

### Enhancements

- Improve error message for LwM2M plugin when object ID is not valid. [#8654](https://github.com/emqx/emqx/pull/8654).
- Add tzdata apk package to alpine docker image. [#8671](https://github.com/emqx/emqx/pull/8671)
- Refine Rule Engine error log. RuleId will be logged when take action failed. [#8737](https://github.com/emqx/emqx/pull/8737)
- Increases the latency interval for MQTT Bridge test connections to improve compatibility in high-latency environments. [#8745](https://github.com/emqx/emqx/pull/8745)
- Close ExProto client process immediately if it's keepalive timeouted. [#8725](https://github.com/emqx/emqx/pull/8725)
- Upgrade grpc-erl driver to 0.6.7 to support batch operation in sending stream. [#8725](https://github.com/emqx/emqx/pull/8725)
- Improved jwt authentication module initialization process. [#8736](https://github.com/emqx/emqx/pull/8736)

### Bug fixes

- Fix rule SQL compare to null values always returns false. [#8743](https://github.com/emqx/emqx/pull/8743)
  Before this change, the following SQL failed to match on the WHERE clause (`clientid != foo` returns false):
  `SELECT 'some_var' as clientid FROM "t" WHERE clientid != foo`.
  The `foo` variable is a null value, so `clientid != foo` should be evaluated as true.
- Fix GET `/auth_clientid` and `/auth_username` counts. [#8655](https://github.com/emqx/emqx/pull/8655)
- Add an idle timer for ExProto UDP client to avoid client leaking [#8628](https://github.com/emqx/emqx/pull/8628)
- Fix ExHook can't be un-hooked if the grpc service stop first. [#8725](https://github.com/emqx/emqx/pull/8725)
- Fix the problem that ExHook cannot continue hook chains execution for mismatched topics. [#8807](https://github.com/emqx/emqx/pull/8807)
- Fix GET `/listeners/` crashes when listener is not ready. [#8752](https://github.com/emqx/emqx/pull/8752)
- Fix repeated warning messages in bin/emqx [#8824](https://github.com/emqx/emqx/pull/8824)


## v4.3.18

### Enhancements

- Upgrade Erlang/OTP from 23.2.7.2-emqx-3 to 23.3.4.9-3 [#8511](https://github.com/emqx/emqx/pull/8511)
- Make possible to debug-print SSL handshake procedure by setting listener config `log_level=debug` [#8553](https://github.com/emqx/emqx/pull/8553)
- Add option to perform GC on connection process after TLS/SSL handshake is performed. [#8649](https://github.com/emqx/emqx/pull/8649)
  Expected to reduce around 35% memory consumption for each SSL connection. See [#8637](https://github.com/emqx/emqx/pull/8637) for more details.

## v4.3.17

### Bug fixes

- Fixed issue where the dashboard APIs were being exposed under the
  management listener. [#8411]

- Fixed crash when shared persistent subscription [#8441]
- Fixed issue in Lua hook that prevented messages from being
  rejected [#8535]
- Fix ExProto UDP client keepalive checking error.
  This causes the clients to not expire as long as a new UDP packet arrives [#8575]

### Enhancements

- HTTP API(GET /rules/) support for pagination and fuzzy filtering. [#8450]
- Add check_conf cli to check config format. [#8486]
- Optimize performance of shared subscription

## v4.3.16

### Enhancements

- Add the possibility of configuring the password for
  password-protected private key files used for dashboard and
  management HTTPS listeners. [#8129]
- Add message republish supports using placeholder variables to specify QoS and Retain values. Set `${qos}` and `${flags.retain}` use the original QoS & Retain flag.
- Add supports specifying the network interface address of the cluster listener & rpc call listener. Specify `0.0.0.0` use all network interfaces, or a particular network interface IP address.
- ExHook supports to customize the socket parameters for gRPC client. [#8314]

### Bug fixes

- Avoid repeated writing `loaded_plugins` file if the plugin enable stauts has not changed [#8179]
- Correctly tally `connack.auth_error` metrics when a client uses MQTT
  3.1. [#8177]
- Do not match ACL rules containing placeholders if there's no
  information to fill them. [#8280]
- Fixed issue in Lua hook that didn't prevent a topic from being
  subscribed to. [#8288]
- Ensuring that exhook dispatches the client events are sequential. [#8311]
- Ensure start dashboard ok event if default_username is missing.
- Fix key update from JWKS server by JWT auth. [#8337]
- Better errors for JWT claim validations. [#8337]

## v4.3.15

### Enhancements

* Refactored `bin/emqx` help messages.
* Upgrade script refuses upgrade from incompatible versions. (e.g. hot upgrade from 4.3 to 4.4 will fail fast).
* Made possible for EMQX to boot from a Linux directory which has white spaces in its path.
* Add support for JWT authorization [#7596]
  Now MQTT clients may be authorized with respect to a specific claim containing publish/subscribe topic whitelists.
* Better randomisation of app screts (changed from timestamp seeded sha hash (uuid) to crypto:strong_rand_bytes)
* Return a client_identifier_not_valid error when username is empty and username_as_clientid is set to true [#7862]
* Add more rule engine date functions: format_date/3, format_date/4, date_to_unix_ts/3, date_to_unix_ts/4 [#7894]
* Add proto_name and proto_ver fields for $event/client_disconnected event.
* Mnesia auth/acl http api support multiple condition queries.
* Inflight QoS1 Messages for shared topics are now redispatched to other alive subscribers upon chosen subscriber session termination.
* Make auth metrics name more understandable.
* Allow emqx_management http listener binding to specific interface [#8005]
* Add rule-engine function float2str/2, user can specify the float output precision [#7991]

### Bug fixes

* List subscription topic (/api/v4/subscriptions), the result do not match with multiple conditions.
* SSL closed error bug fixed for redis client.
* Fix mqtt-sn client disconnected due to re-send a duplicated qos2 message
* Rule-engine function hexstr2bin/1 support half byte [#7977]
* Shared message delivery when all alive shared subs have full inflight [#7984]
* Improved resilience against autocluster partitioning during cluster
  startup. [#7876]
  [ekka-158](https://github.com/emqx/ekka/pull/158)
* Add regular expression check ^[0-9A-Za-z_\-]+$ for node name [#7979]
* Fix `node_dump` variable sourcing. [#8026]
* Fix heap size is growing too fast when trace large message.
* Support customized timestamp format of the log messages.

## v4.3.14

### Enhancements

* Add `RequestMeta` for exhook.proto in order to expose `cluster_name` of emqx in each gRPC request. [#7524]
* Support customize emqx_exhook execution priority. [#7408]
* add api: PUT /rules/{id}/reset_metrics.
  This api reset the metrics of the rule engine of a rule, and reset the metrics of the action related to this rule. [#7474]
* Enhanced rule engine error handling when json parsing error.
* Add support for `RSA-PSK-AES256-GCM-SHA384`, `RSA-PSK-AES256-CBC-SHA384`,
 `RSA-PSK-AES128-GCM-SHA256`, `RSA-PSK-AES128-CBC-SHA256` PSK ciphers, and remove `PSK-3DES-EDE-CBC-SHA`,
 `PSK-RC4-SHA` from the default configuration. [#7427]
* Diagnostic logging for mnesia `wait_for_table`
  - prints check points of mnesia internal stats
  - prints check points of per table loading stats
  Help to locate the problem of long table loading time.
* Add `local` strategy for Shared Subscription.
  That will preferentially dispatch messages to a shared subscriber at the same
  node. It will improves the efficiency of shared messages dispatching in certain
  scenarios, especially when the emqx-bridge-mqtt plugin is configured as shared
  subscription. [#7462]
* Add some compression functions to rule-engine: gzip, gunzip, zip, unzip, zip_compress, zip_uncompress

### Bug fixes

* Prohibit empty topics in strict mode
* Make sure ehttpc delete useless pool always succeed.
* Update mongodb driver to fix potential process leak.
* Fix a potential security issue #3155 with emqx-dashboard plugin.
  In the earlier implementation, the Dashboard password is reset back to the
  default value of emqx_dashboard.conf after the node left cluster.
  Now we persist changed password to protect against reset. [#7518]
* Silence grep/sed warnings in docker-entrypoint.sh. [#7520]
* Generate `loaded_modules` and `loaded_plugins` files with default values when no such files exists. [#7520]
* Fix the configuration `server_name_indication` set to disable does not take effect.
* Fix backup files are not deleted and downloaded correctly when the API path has ISO8859-1 escape characters.

## v4.3.13

### Important changes

* For docker image, /opt/emqx/etc has been removed from the VOLUME list,
  this made it easier for the users to rebuild image on top with changed configs.
* CentOS 7 Erlang runtime is rebuilt on OpenSSL-1.1.1n (previously on 1.0),
  Prior to v4.3.13, EMQX pick certain cipher suites proposed by the clients,
  but then fail to handshake resulting in a `malformed_handshake_data` exception.
* CentOS 8 Erlang runtime is rebuilt on RockyLinux 8.
  'centos8' will remain in the package name to keep it backward compatible.

### Enhancements

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
  this change. [#7336]
* Return the cached resource status when querying a resource using HTTP APIs.
  This is to avoid blocking the HTTP request if the resource is unavailable. [#7374]

### Bug fixes

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
* Auto subscribe to an empty topic will be simply ignored now

## v4.3.12
### Important changes

### Minor changes
* Fix updating `emqx_auth_mnesia.conf` password and restarting the new password does not take effect [#6717]
* Fix import data crash when emqx_auth_mnesia's record is not empty [#6717]
* Fix `os_mon.sysmem_high_watermark` may not alert after reboot.
* Enhancement: Log client status before killing it for holding the lock for too long.
  [emqx-6959](https://github.com/emqx/emqx/pull/6959)
  [ekka-144](https://github.com/emqx/ekka/pull/144)
  [ekka-146](https://github.com/emqx/ekka/pull/146)

## v4.3.11

Important notes:

- For Debian/Ubuntu users

  We changed the package installed service from init.d to systemd.
  The upgrade from init.d to systemd is verified, however it is
  recommended to verify it before rolling out to production.
  At least to ensure systemd is available in your system.

- For Centos Users

  RPM package now depends on `openssl11` which is NOT available
  in certain centos distributions.
  Please make sure the yum repo [epel-release](https://docs.fedoraproject.org/en-US/epel) is installed.

### Important changes

* Debian/Ubuntu package (deb) installed EMQX now runs on systemd [#6389]<br>
  This is to take advantage of systemd's supervision functionality to ensure
  EMQX service is restarted after crashes.

### Minor changes

* Clustering malfunction fixes [#6221, #6381]
  Mostly changes made in [ekka](https://github.com/emqx/ekka/pull/134)<br>
  From 0.8.1.4 to 0.8.1.6, fixes included intra-cluster RPC call timeouts,<br>
  also fixed `ekka_locker` process crashed after killing a hanged lock owner.

* Improved log message when TCP proxy is in use but proxy_protocol configuration is not turned on [#6416]<br>
  "please check proxy_protocol config for specific listeners and zones" to hint a misconfiguration

* Helm chart supports networking.k8s.io/v1 [#6368]

* Fix session takeover race condition which may lead to message loss [#6396]

* EMQX docker images are pushed to aws public ecr in an automated CI job [#6271]<br>
  `docker pull public.ecr.aws/emqx/emqx:4.3.10`

* Fix webhook URL path to allow rule-engine variable substitution [#6399]

* Corrected RAM usage display [#6379]

* Changed emqx_sn_registry table creation to runtime [#6357]<br>
  This was a bug introduced in 4.3.3, in which the table is changed from ets to mnesia<br>
  this will cause upgrade to fail when a later version node joins a 4.3.0-2 cluster<br>

* Log level for normal termination changed from info to debug [#6358]

* Added config `retainer.stop_publish_clear_msg` to enable/disable empty message retained message publish [#6343]<br>
  In MQTT 3.1.1, it is unclear if a MQTT broker should publish the 'clear' (no payload) message<br>
  to the subscribers, or just delete the retained message. So we have made it configurable

* Fix mqtt bridge malfunction when remote host is unreachable (hangs the connection) [#6286, #6323]

* System monitor now inspects `current_stacktrace` of suspicious process [#6290]<br>
  `current_function` was not quite helpful

* Changed default `max_topc_levels` config value to 128 [#6294, #6420]<br>
  previously it has no limit (config value = 0), which can be a potential DoS threat

* Collect only libcrypto and libtinfo so files for zip package [#6259]<br>
  in 4.3.10 we tried to collect all so files, however glibc is not quite portable

* Added openssl-1.1 to RPM dependency [#6239]

* Http client duplicated header fix [#6195]

* Fix `node_dump` issues when working with deb or rpm installation [#6209]

* Pin Erlang/OTP 23.2.7.2-emqx-3 [#6246]<br>
  4.3.10 is on 23.2.7.2-emqx-2, this bump is to fix an ECC signature name typo:
  ecdsa_secp512r1_sha512 -> ecdsa_secp521r1_sha512

* HTTP client performance improvement [#6474, #6414]<br>
  The changes are mostly done in the dependency [repo](https://github.com/emqx/ehttpc).

* For messages from gateways add message properties as MQTT message headers [#6142]<br>
  e.g. messages from CoAP, LwM2M, Stomp, ExProto, when translated into MQTT message<br>
  properties such as protocol name, protocol version, username (if any) peer-host<br>
  etc. are filled as MQTT message headers.

* Format the message id to hex strings in the log message [#6961]

## v4.3.0~10

Older version changes are not tracked here.
