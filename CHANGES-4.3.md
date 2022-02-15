# EMQ X 4.3 Changes

Started tracking changes in CHANGE.md since EMQ X v4.3.11

NOTE: Keep prepending to the head of the file instead of the tail

File format:

- Use weight-2 heading for releases
- One list item per change topic
  Change log ends with a list of github PRs

## v4.3.13

### Enhancements

* CLI `emqx_ctl pem_cache clean` to force purge x509 certificate cache,
  to force an immediate reload of all certificates after the files are updated on disk.

### Bug fixes

* Fix case where publishing to a non-existent topic alias would crash the connection [#6979]
* Fix HTTP-API 500 error on querying the lwm2m client list on the another node [#7009]

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

* Debian/Ubuntu package (deb) installed EMQ X now runs on systemd [#6389]<br>
  This is to take advantage of systemd's supervision functionality to ensure
  EMQ X service is restarted after crashes.

### Minor changes

* Clustering malfunction fixes [#6221, #6381]
  Mostly changes made in [ekka](https://github.com/emqx/ekka/pull/134)<br>
  From 0.8.1.4 to 0.8.1.6, fixes included intra-cluster RPC call timeouts,<br>
  also fixed `ekka_locker` process crashed after killing a hanged lock owner.

* Improved log message when TCP proxy is in use but proxy_protocol configuration is not turned on [#6416]<br>
  "please check proxy_protocol config for specific listeners and zones" to hint a misconfiguration

* Helm chart supports networking.k8s.io/v1 [#6368]

* Fix session takeover race condition which may lead to message loss [#6396]

* EMQ X docker images are pushed to aws public ecr in an automated CI job [#6271]<br>
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
