# EMQX ACME Plugin

ACME client plugin for automatic TLS certificate issuance and renewal.

This plugin integrates with ACME-compatible certificate authorities (e.g. Let's Encrypt)
to automatically issue and renew TLS certificates for EMQX SSL listeners.
Certificates are stored in EMQX managed certificate bundles.


> ⚠️ **NOTE — persist `<data_dir>/certs2/` across EMQX redeployments.**
>
> Everything the plugin needs to keep across restarts lives under
> `<data_dir>/certs2/global/<cert_bundle_name>/`:
>
> - `chain.pem` + `key.pem` — the issued *certificate bundle*. Lose
>   them and the plugin will reissue from scratch on next start,
>   counting against Let's Encrypt's per-domain "5 duplicate
>   certificates per week" rate limit.
> - `acc-key.pem` — the ACME *account key*, the identity Let's Encrypt
>   registered for you. Lose it and every redeploy starts a brand new
>   account, burning through "10 newAccount calls per IP per 3 hours"
>   and orphaning your ability to *revoke* the previous account's
>   certs.
>
> `<data_dir>` is `/opt/emqx/data` on docker, `/var/lib/emqx` on
> DEB/RPM. In Docker, bind-mount the whole `data/` (or at least
> `data/certs2/`) to a host volume; in Kubernetes, back it with a PVC.
> The plugin generates the account key inside the bundle on first
> issuance and replicates it to every cluster node automatically via
> `emqx_managed_certs`, so a fresh cluster boots without manual key
> distribution.

## Quick Start

Minimal single-node setup for a publicly-resolvable domain:

1. **Install and enable** the plugin from the EMQX dashboard (*Management → Plugins*).
2. **Configure** the plugin (only the four fields below need attention; everything
   else can stay at defaults):
   - `domains = "mqtt.example.com"` — comma-separated list (each domain
     must publicly resolve to this node).
   - `contact = "mailto:admin@example.com"` — comma-separated list of
     contact addresses for CA renewal / revocation notices.
   - `challenge_port = 5080` (or any high port EMQX can bind) — put a
     reverse proxy or iptables redirect in front so the public `:80` reaches
     it. See [Reaching port 80](#reaching-port-80).
   - `dir_url` — leave at the default (LE production) or switch to staging
     while you verify everything works.
3. **Click *Issue / Renew Now*** in the plugin UI. On the *first* issuance
   (the bundle is empty) the plugin will:
   - Auto-generate the ACME account key at `acc_key` if the file is absent.
   - Issue the certificate via HTTP-01.
   - Rewrite each listener in `listener_ids` (default `ssl:default` and
     `wss:default`) to point at the new bundle, so MQTT TLS starts using
     it without further config.
   - Auto-create the dashboard HTTPS listener on `:18084` with the same
     certificate (because `enable_dashboard_https = true` by default).

   On every subsequent run (renewals, or *Issue / Renew Now* once a bundle
   already exists) the plugin only refreshes the bundle files in place;
   listener configs and the dashboard HTTPS setup are left alone, and
   Erlang's SSL PEM cache reloads the new cert without restarting listeners.
4. **Reload the dashboard** over `https://your.domain:18084/`, log in, and
   from the plugin UI click *Disable Dashboard HTTP Listener*. The button
   refuses to do anything unless you're already on HTTPS — once it succeeds,
   plaintext `:18083` is gone cluster-wide. **This is the recommended
   posture for any production deployment**: keeping the dashboard HTTP
   listener around defeats the point of having the cert.

That's it. Renewals happen automatically via the periodic
`check_interval_hours` poll; no further intervention needed.

## How It Works

1. The plugin registers an ACME account (or reuses an existing one) with the configured CA.
2. It starts an ephemeral HTTP listener to respond to HTTP-01 challenges during issuance.
3. The issued certificate chain and private key are stored in a managed cert bundle.
   The ACME account key lives separately at the operator-managed `acc_key` path
   (see [ACME account key](#acme-account-key)) and is never written into the bundle.
4. SSL listeners reference the bundle via `ssl_options.managed_certs.bundle_name` -
   `listener_ids` lets the plugin rewrite that field for you on first issuance.
5. A periodic check (every `check_interval_hours`) renews the certificate when it
   expires within `renew_before_expiry_days`. Renewals refresh the bundle files in
   place; Erlang's SSL PEM cache picks up the new cert without restarting listeners.

## Example config

The per-field reference lives in the plugin's avsc schema and is rendered
inline in the dashboard's plugin config form (with the i18n descriptions
for each field). The HOCON example below shows a typical shape; for
field-level docs, hover the field labels in the dashboard.

```hocon
dir_url = "https://acme-v02.api.letsencrypt.org/directory"
# Comma-separated list of SAN domains for the cert.
domains = "mqtt.example.com,mqtt2.example.com"
# Comma-separated list of CA contact addresses (renewal/revocation notices).
contact = "mailto:admin@example.com,mailto:ops@example.com"
cert_bundle_name = "acme"
# Comma-separated list of listener IDs to migrate (each "ssl:<name>" or "wss:<name>").
listener_ids = "ssl:default,wss:default"
cert_type = "ec"
# High port EMQX can bind; reverse-proxy or iptables-redirect 80 -> this.
challenge_port = 5080
renew_before_expiry_days = 30
check_interval_hours = 24
enable_dashboard_https = true
dashboard_https_port = 18084
# acc_key is left unset; the plugin manages it inside the cert bundle.
```

Then configure an SSL listener to use the bundle (or, for listeners named in
`listener_ids`, the plugin will rewrite this for you on the first issuance):

```hocon
listeners.ssl.default {
  bind = "0.0.0.0:8883"
  ssl_options {
    managed_certs {
      bundle_name = "acme"
    }
  }
}
```

## ACME account key

In RFC 8555, the ACME account private key *is* the account identity — the
client generates the key locally, sends a `newAccount` request signed with it,
and the CA creates the account on the spot. There is no portal where you
"register a key" out-of-band.

**Default behavior (recommended for almost all deployments):** leave
`acc_key` unset. On the first issuance the plugin generates a fresh EC P-256
(or RSA-2048 if `cert_type = "rsa"`) key in memory, then stores it via
`emqx_managed_certs:add_managed_files/3`, which writes it to
`<data_dir>/certs2/global/<cert_bundle_name>/acc-key.pem` on **every**
cluster node — so the cluster's ACME identity is replicated automatically,
no manual key distribution required. Subsequent issuances reuse the same
file in place. Lifecycle: the account key sits next to the cert chain in
the same bundle directory; bind-mount or PVC-back the data dir and you're
covered (see the persistence note at the top).

**Operator override (`acc_key` set):** use this when you must pin the key
to an out-of-bundle path — e.g. a Kubernetes Secret mounted at a known
location, or a key shared with another piece of software. Set `acc_key` to
the `file://` URI of the PEM. The plugin reads the file on every issuance
and never overwrites it; if the file is absent on the local node it
auto-generates one (this is **not** replicated cluster-wide, so for clusters
you must pre-distribute the file yourself). If the PEM is encrypted, also
set `acc_key_password` to either a `file://` URI pointing at a plain-text
password file (recommended — keeps the secret off-config; a trailing newline
is trimmed) or an inline password string (used verbatim). `${EMQX_ETC_DIR}`
/ `${VAR}` are expanded at use time on `file://` paths so the same config
works across docker and DEB/RPM; inline passwords are taken literally with no
env-var interpolation.

## Reaching port 80

ACME CAs always perform the HTTP-01 challenge against port 80 of the
domain being validated — that's fixed by RFC 8555 and not configurable
on the CA side. EMQX, however, runs as a non-root user (`emqx`) and
cannot bind ports below 1024, so setting `challenge_port = 80`
typically fails with `eacces`.

The supported pattern is to point `challenge_port` at a high port that
EMQX *can* bind (e.g. `5080`) and put one of the following in front so
that the public `domain:80` reaches EMQX's `challenge_port`:

- **Reverse proxy.** Run nginx/caddy/HAProxy on the same host (as
  root or via a CAP_NET_BIND_SERVICE capability) and proxy
  `http://domain/.well-known/acme-challenge/*` to
  `http://127.0.0.1:<challenge_port>`. Other paths can return 404.
- **Port forwarding.** On Linux, redirect inbound 80 to the high port
  with iptables:

      iptables -t nat -A PREROUTING -p tcp --dport 80 \
                      -j REDIRECT --to-port 5080

  Or use `socat`/`systemd` socket activation to bridge the two ports.
- **Kernel privilege.** Grant the EMQX binary the
  `CAP_NET_BIND_SERVICE` capability so it can bind 80 directly:

      setcap 'cap_net_bind_service=+ep' \
             /opt/emqx/erts-*/bin/beam.smp

  This is OS- and packaging-specific and is not recommended in
  containerised deployments — prefer the reverse proxy pattern.

## Prerequisites

- The domain(s) must resolve to the EMQX node's public IP.
- Port 80 (or the configured `challenge_port`) must be reachable from the internet
  for HTTP-01 challenge validation.
- For staging/testing, use the Let's Encrypt staging URL:
  `https://acme-staging-v02.api.letsencrypt.org/directory`

## API Endpoints

Available via the plugin API gateway at `/api/v5/plugin_api/emqx_acme-<version>/`.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/status` | Current state: `domains`, `cert_bundle_name`, `in_progress`, `last_result`, `last_check`, and `certificate` (issuer, subject, not_after, file paths). |
| POST | `/issue` | Kick off issuance asynchronously. Returns `202 {"result":"started"}`; poll `/status` for the outcome. `409` if another action is already running. |
| POST | `/renew` | Same shape as `/issue`, but for renewal. |
| POST | `/disable_dashboard_http` | Set `dashboard.listeners.http.bind = 0` cluster-wide, stopping the plaintext listener. Refuses with `409 NO_HTTPS_LISTENER` if no dashboard HTTPS listener is configured. |

The plugin UI under `/ui` uses these endpoints; you don't normally call them by hand.

## FAQ

### Issuance worked against Let's Encrypt staging but fails against production

Symptom — issuance fails with an error containing this string:

> `During secondary validation: DNS problem: query timed out looking up A for ...`

Cause — Let's Encrypt **production** runs *multi-perspective validation*:
each challenge is re-checked from several geographically distinct LE
network points and **all** of them must succeed. The `"During secondary
validation"` marker means the primary validator reached your challenge
listener but at least one secondary validator (often in another region)
could not resolve your domain in time. LE staging only does the primary
check, which is why the same setup succeeds against staging.

This is almost always a property of the DNS provider, not of the EMQX/plugin
path. Free dynamic-DNS services (DuckDNS, No-IP, Dynu, …) typically run a
small number of authoritative servers without regional replication or
anycast; far-away resolvers see slow or jittery lookups that exceed LE's
~10-second budget per perspective.

Fixes:
- **Use a DNS provider with global presence** (Cloudflare, Route 53,
  Google Cloud DNS, NS1, …) on a real domain. The plugin and Let's
  Encrypt staging path are unchanged.
- **Verify resolution from multiple regions** before retrying production —
  e.g. `dig @8.8.8.8 your.domain`, `dig @1.1.1.1 your.domain`, plus a
  few of LE's [public test perspectives](https://letsdebug.net).
- **Don't spam retries.** LE production rate-limits validation
  failures (5 failed authorizations per account per hostname per hour);
  burning through them turns "occasional flake" into "locked out for an
  hour." Get DNS reliable first, then issue.
