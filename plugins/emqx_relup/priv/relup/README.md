# priv/relup catalog

One `.relup` file per supported `{From, Target}` hop. The handler
reads every `*.relup` file under this directory via `file:script/1`
(so dynamic terms are allowed) and picks the entry whose
`{from_version, target_version}` matches the requested hop. Filenames
are arbitrary; `<from>-to-<to>.relup` is the convention.

## Supported Upgrade Paths

| From | To | Changes |
|---|---|---|
| 5.10.4 | 5.10.5 | [5.10.4-5.10.5](#5104-5105) |

## Changes

### 5.10.4-5.10.5

- Restart `esaml` so hot-upgraded nodes pick up SAML XXE protection.
- Load dashboard/data-backup modules and re-announce `emqx` BPAPI so backup-file download authorization changes take effect for API-key callers.
- Stop Redis resources, restart `eredis`, reload `emqx_redis`, then start Redis resources so Sentinel managers are recreated with isolated manager names.

## Schema

```erlang
#{
    from_version => "5.10.4",
    target_version => "5.10.5",
    code_changes => [
        % must always reload emqx_release module
        {load_module, emqx_release},
        {load_module, another_module},
        {update, emqx_other, {advanced, ExtraTerm}},
        {apply, emqx_bpapi, announce, [node(), emqx]},
        {restart_application, emqx_something}
    ],
    post_upgrade_callbacks => [
        pr_NNNNN_some_callback,
        {pr_NNNNN_callback_with_args, ["5.10.5", ChildSpec]}
    ]
}.
```

Both `from_version` and `target_version` are strings (not binaries).
`code_changes` and `post_upgrade_callbacks` may be empty lists.

## `code_changes`

Applied in order, before any `post_upgrade_callbacks`. Supported
instruction shapes:

| Instruction | Effect |
|---|---|
| `{load_module, Mod}` | Replaces the running beam for `Mod`. Skipped if the new beam's md5 matches what's already loaded. Equivalent to `code:soft_purge/1` + `code:load_binary/3`. |
| `{update, Mod, {advanced, Extra}}` | OTP-style stateful upgrade for processes whose callback module is `Mod`: `sys:suspend` → `load_module` → `sys:change_code(_, Mod, FromVsn, Extra)` → `sys:resume`. Use when a `gen_server` (or other `sys`-aware process) holds state that needs migrating. |
| `{restart_application, App}` | `application:stop` → purge & delete every module of `App` → reload the target release's `.app` spec → `application:ensure_all_started`. Use when stateless restart is acceptable and many modules changed. |
| `{apply, Mod, Fun, Args}` | Calls `erlang:apply(Mod, Fun, Args)` inside `code_changes`, in catalog order, and requires the callback to return `ok`. Use only for idempotent pre/post steps that must run before later code changes. |

Prefer `post_upgrade_callbacks` unless a callback must run between
two `code_changes`. Put one-off `{apply, ...}` helpers in this plugin
rather than `emqx_post_upgrade`, because they are not post-upgrade callbacks.

The handler injects `code:add_patha(<RootDir>/data/patches)` after
all `code_changes` complete. Beams dropped into `data/patches` will
be discoverable in `code:get_path/0` for the rest of the node's
lifetime; this is the supported hot-patch escape hatch.

## `post_upgrade_callbacks`

Each entry is either:

- An atom `Func` → invoked as `Mod:Func(FromVsn)`.
- A tuple `{Func, Args}` → invoked as `Mod:Func(FromVsn, Arg1, Arg2, …)`.

`Mod` is resolved per-hop via `emqx_relup_handler:get_upgrade_mod/1`:

1. If `emqx_post_upgrade_<TargetVsn>` is loaded → use it.
   (`<TargetVsn>` is the literal target version string — not a
   normalised one; e.g. for target `5.10.5` the module name is
   `emqx_post_upgrade_5.10.5`. Atom-quoting is required when
   declaring the module: `-module('emqx_post_upgrade_5.10.5').`)
2. Otherwise → use `emqx_post_upgrade` (shipped with EMQX itself,
   in `apps/emqx/src/emqx_post_upgrade.erl`).

### The "must exist on the running node" trap

`post_upgrade_callbacks` runs **after** `code_changes`. The version
of `emqx_post_upgrade` that handles the call is therefore whatever
is loaded at that point — which is:

- **The new** `emqx_post_upgrade` if `code_changes` includes
  `{load_module, emqx_post_upgrade}` (or a `{restart_application,
  emqx}`).
- **The old** `emqx_post_upgrade` otherwise.

**Adding a `pr_NNNNN_*` function only to the target's
`emqx_post_upgrade.erl` is not enough.** If the relup hop's
`code_changes` doesn't reload `emqx_post_upgrade`, the post-upgrade
phase calls the *old* module, the new function isn't there, and the
hop fails (and the VM restarts, since exceptions in the
post-upgrade phase are not caught above the callback level).

There are two safe authoring patterns:

1. **Reload `emqx_post_upgrade` first.** Put `{load_module,
   emqx_post_upgrade}` near the end of `code_changes` — after any
   `{restart_application, emqx}` that would purge it, and before
   `post_upgrade_callbacks` references the new function.

2. **Ship the callback in the relup plugin itself.** Add a
   `emqx_post_upgrade_<TargetVsn>.erl` module under
   `plugins/emqx_relup/src/`. The relup *plugin* is already started
   on the running node before the upgrade triggers, so the module
   is loaded *now* — it doesn't need to come from the new release.
   `get_upgrade_mod/1` prefers it over `emqx_post_upgrade`.

   This is the right place for one-off callbacks that don't make
   sense to carry forward in the EMQX source tree.

### Reentrancy

Callbacks must be **idempotent**. The operator may retry a failed
hop, and a callback that already ran on the previous attempt may
run again. Guard against double-effects (re-creating a child spec
that's already running, re-issuing a migration that's already
applied, etc.).

The reference example is `pr_16802_restart_rabbitmq_connectors/1`
in `apps/emqx/src/emqx_post_upgrade.erl`: it filters to running
RabbitMQ connectors before restarting them, so a re-run on a
healthy cluster is a no-op.

## End-to-end ordering (single hop)

```
1. handler verifies <tarball>.sha256 and extracts to a temp dir
2. handler copies libs and release files into <RootDir>/relup/<TargetVsn>/
3. eval_code_changes: for each instruction, load_module /
   update / restart_application / apply
4. add_patha(<RootDir>/data/patches)             — last step of (3)
5. eval_post_upgrade_actions: for each entry,
       erlang:apply(get_upgrade_mod(TargetVsn), Func, [FromVsn | Args])
6. handler writes <RootDir>/relup/current with TargetVsn
```

Failure semantics:

| Stage | On failure |
|---|---|
| 1–2 | `{error, #{stage => check_and_unpack, ...}}` (HTTP 400). |
| 3–4 | **VM restart via `init:restart/0`** — exceptions in `eval_code_changes` are not caught. Operator must investigate from logs. |
| 5 | `{error, #{stage => perform_upgrade, ...}}` (HTTP 500). The `eval_post_upgrade_actions` wrapper turns any callback exception into a returned error, so the VM stays up. |
| 6 | `{error, #{stage => permanent_upgrade, ...}}` (HTTP 500). |
