# EMQX plugins

EMQX is built as a set of Erlang/OTP applications under `apps/`.
In this monorepo, `mix` (Elixir build tooling) is used to compile and test those apps together.

For plugin work in this repository, you will typically keep both files in the plugin app:

- `mix.exs`: used by monorepo compile/test workflows.
- `rebar.config`: used to build plugin packages (`emqx_plugrel`) as `.tar.gz` artifacts.

Important: building a plugin package does not auto-boot it in EMQX.
Runtime plugin lifecycle is managed with:
`emqx ctl plugins install|enable|start`.

## Prepare

To build a EMQX plugin, you will need `rebar3` and the emqx-plugin template.

You can run `make ensure-rebar3` in this project root to ensure `rebar3` is downloaded to `./`.

If `rebar3 new help` does not list `emqx-plugin (custom)`, you will need to install the template first:

```bash
mkdir -p ~/.config/rebar3/templates
cd ~/.config/rebar3/templates
git clone https://github.com/emqx/emqx-plugin-template.git
```

If `REBAR_CACHE_DIR` is set, use:

```bash
mkdir -p "$REBAR_CACHE_DIR/.config/rebar3/templates"
cd "$REBAR_CACHE_DIR/.config/rebar3/templates"
git clone https://github.com/emqx/emqx-plugin-template.git
```

Then verify:

```bash
rebar3 new help
```

With the template in place, there are two common development modes:

## Build as a standalone plugin project

Follow the instructions in the template project:
[emqx-plugin-template](https://github.com/emqx/emqx-plugin-template).

## Build in this monorepo (`contribute-*` branches)

### Bootstrap

- Pick a plugin name. It should be globally unique and also be the Erlang app name.
- Check out a `contribute-*` branch.
  Example: use `contribute-61` for EMQX 6.1-based plugin work.
- Generate the app with the template:
  `rebar3 new emqx-plugin {plugin_name}`
- Place the generated plugin app under `apps/{plugin_name}`.
- Add `apps/{plugin_name}/mix.exs` so the app participates in monorepo build/test.
  You can take `apps/emqx_username_quota/mix.exs` as a reference.

### Development and test

- Implement plugin code in `apps/{plugin_name}/src`.
- Add Common Test suites in `apps/{plugin_name}/test`.
- Run tests with the app-level CT target:
  `make apps/{plugin_name}-ct`

Example:
`make apps/emqx_username_quota-ct`

### Integration test and release

- For quick local integration testing (without adding plugin apps to EMQX boot apps), run:
  `scripts/run-plugin-dev.sh {plugin_name} [--attach]`
- Build the plugin package from repo root:
  `make plugin-{plugin_name}`

### Example

- Plugin name: `emqx_username_quota`
- App path: `apps/emqx_username_quota`
- Package build command: `make plugin-emqx_username_quota`
