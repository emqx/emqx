# EMQX hot-upgrade

This plugin applies a `.relup` set of code-change instructions to a
running EMQX node. Operators drive it via the `emqx ctl relup ...`
CLI on each node.

## Operator workflow

1. **Install the plugin**
   The plugin tarball is published per EMQX release at:

       `https://packages.emqx.io/emqx-plugins/e<EMQX-VSN>/emqx_relup-<PLUGIN-VSN>.tar.gz`

    Install the plugin from the dashboard.

2. **Confirm the upgrade path is supported.**
   `emqx ctl relup list-supported-paths` lists the supported
   `{from, target}` hops the plugin's `priv/relup/` catalog knows
   about.

3. **Copy the target tarball + sha256 sidecar to every node.** Two
   files, anywhere the EMQX process can read:
   - `emqx-enterprise-<TargetVsn>-<os>-<arch>.tar.gz` - the EMQX
     target release.
   - `<tarball>.sha256` - sha256 digest of the tarball. The
     `sha256sum` output format (`<digest>  <filename>`) is accepted.

4. **Trigger the upgrade.** `emqx ctl relup upgrade <TarballPath> [--force]`
   on each node. The handler:
   - Verifies `<TarballPath>.sha256` against the tarball's actual
     digest - refuses to extract on mismatch.
   - Refuses if `data/patches/` contains any `*.beam`. Those would
     shadow modules from the upgrade target (the patches dir is
     prepended to the code path via `vm.args -pa`), so if the new
     release already supersedes a hot-patched module, the stale beam
     would silently keep running. Delete the patches first, or pass
     `--force` if you intend them to remain applied on top of the
     target.
   - Extracts the tarball and reads `REL_VSN` from
     `releases/emqx_vars`.
   - Looks up the matching `{from, target}` hop in
     `priv/relup/*.relup` and runs the code-change instructions and
     post-upgrade callbacks.

   Cluster-wide rollout is the operator's responsibility.

5. **Verify the node** before moving on. Two cheap signals:
   - `emqx ctl status` reports the node running.
   - `cat <RootDir>/relup/current` matches the target version, and
     `<RootDir>/relup/<TargetVsn>/` contains `bin/`, `erts-*/`,
     `lib/`, `releases/`.

   On the next `emqx start`/`restart`, the `bin/emqx` wrapper
   detects `relup/current` and execs into the deployed tree (new
   ERTS, new bin scripts, new lib). The original `<RootDir>` stays
   the authority for `data/`, `etc/`, `log/`, `plugins/`.

6. **Clean up after success.** Manual `rm` once the cluster is fully
   on the target version: the tarball, the `.sha256` sidecar, and
   whatever directory you parked them in. The plugin doesn't track
   the source path, so no leftover state to clean inside the plugin.

## Rollback

There is no in-place rollback for an applied hop. The hot-upgrade
runs `code_changes` against the live VM and any
`post_upgrade_callbacks` may have mutated data on disk; reversing
that is not supported by this plugin.

Practical rollback paths:

- **Before the next restart**, if the upgrade succeeded but the new
  code is misbehaving and the data on disk is still compatible with
  the old release: `rm <RootDir>/relup/current` (and optionally
  `rm -rf <RootDir>/relup/<TargetVsn>/`), then `emqx restart`. The
  wrapper falls back to the original `<RootDir>/bin/emqx` tree.
  This recovers only the *boot path*; live state inside the running
  VM at the time of the bad upgrade is already gone.
- **Otherwise**, restore from the pre-upgrade backup of `data/`
  (mnesia, configs, etc.) and reinstall the old EMQX release. Plan
  the upgrade window with this in mind — there is no in-place undo.

## Developer Notes

The plugin validates every `priv/relup/*.relup` at app start and
logs a warning for malformed entries; bad files are skipped, not
fatal.

To author a new hop, for each release that needs one:

1. Add `priv/relup/<from>-to-<to>.relup` declaring the
   `code_changes` and `post_upgrade_callbacks` for the hop. See
   `priv/relup/README.md` for the schema, supported instructions,
   and the **post-upgrade callback contract** (in particular: when
   adding a `pr_NNNNN_*` callback to the new EMQX's
   `emqx_post_upgrade`, the relup hop must reload that module
   before invoking the callback — or ship the callback in this
   plugin as `emqx_post_upgrade_<TargetVsn>.erl`).
2. Bump this plugin's `VERSION` and re-publish.
