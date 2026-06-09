Removed the hot-upgrade REST API endpoints (`/api/v5/relup/*`).
Hot-upgrade is now operated exclusively through the `emqx ctl relup`
CLI on each node — there is no dashboard surface.

The target release tarball can be placed anywhere readable by the
EMQX process (no special staging directory). Trigger with `emqx ctl
relup upgrade <TarballPath>`. A `<TarballPath>.sha256` sidecar is
required next to the tarball; the target version is read from the
tarball's own `releases/emqx_vars` (`REL_VSN`).
