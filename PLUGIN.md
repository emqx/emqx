# EMQX Plugin Development Guide

EMQX is organized as a set of Erlang/OTP applications under the `apps/` directory.
Plugin applications in this monorepo live under the `plugins/` directory.

There are two project styles for building an EMQX plugin: standalone (outside this
repository), or embedded in the EMQX monorepo under `plugins/<plugin_app>/`. Both
styles use `rebar3` as the build tool in EMQX 5.x.

---

## Build Files Overview

### Standalone Plugin Project

A standalone EMQX plugin is built and released with `rebar3`.

- `rebar.config` — drives compile/test and builds the plugin package
  (`emqx_plugrel`) as a `.tar.gz` artifact.

This mode is recommended for independent plugin development outside the EMQX
monorepo.

### Plugin Inside the EMQX Monorepo

When a plugin is developed inside the EMQX monorepo under `plugins/<plugin_app>/`,
it is still a self-contained rebar3 project: each plugin carries its own
`rebar.config`, its own `_build/` directory, and resolves its own dependencies.
The monorepo tooling simply wraps the plugin's rebar3 invocation and collects the
resulting artifacts into `_build/plugins/`.

#### Required files under `plugins/<plugin_app>/`

- `VERSION` — single source of truth for plugin version. Referenced from
  `rebar.config` (`{file, "VERSION"}`) and from `src/<plugin_app>.app.src`.
- `rebar.config` — compile configuration, test profile dependencies, `relx`
  release spec, and an `emqx_plugrel` metadata block.
- `src/<plugin_app>.app.src` — OTP application metadata.
- `src/` — Erlang source files.

#### `rebar.config` requirements

For `make plugin-<plugin_app>` (which invokes `rebar3 as emqx_plugrel tar`) to
work, the plugin's `rebar.config` must:

- Declare `emqx_plugrel` under `{plugins, [...]}` so the rebar3 provider
  `rebar3 as emqx_plugrel tar` is available.
- Declare runtime deps (e.g. `emqx_plugin_helper`) under `{deps, [...]}` and
  test-only deps under `{profiles, [{test, [{deps, [...]}]}]}`.
- Define a `relx` release whose name is the plugin app name and whose version
  is read from `VERSION`.
- Define an `emqx_plugrel` keyword list with plugin metadata (`authors`,
  `builder`, `repo`, `functionality`, `compatibility`, `description`).

Reference implementation: `plugins/emqx_offline_messages/rebar.config`.

> **Note**
> Building a plugin package does **not** automatically load or start the plugin in EMQX.
> Runtime plugin lifecycle is managed explicitly via:
>
>     emqx ctl plugins install|enable|start

---

## Preparation

### Ensure `rebar3`

From the repository root:

    make ensure-rebar3

### Install the Plugin Template (standalone only)

If you want to scaffold a brand-new standalone plugin project, check whether the
EMQX plugin template is installed:

    rebar3 new help

If `emqx-plugin (custom)` is not listed, install the template:

    mkdir -p ~/.config/rebar3/templates
    cd ~/.config/rebar3/templates
    git clone https://github.com/emqx/emqx-plugin-template.git

If `REBAR_CACHE_DIR` is set, substitute `$REBAR_CACHE_DIR/.config/rebar3/templates`
for the destination above.

Verify:

    rebar3 new help

---

## Development Modes

### Standalone Plugin Development

For plugins developed outside the EMQX monorepo:

1. Generate a new plugin project:

        rebar3 new emqx-plugin {plugin_name}

2. Develop and test the plugin using `rebar3`.

3. Build the plugin package following the instructions in:
   <https://github.com/emqx/emqx-plugin-template>

### Plugin Development Inside the EMQX Monorepo

This mode is intended for plugin development tightly coupled with a specific
EMQX version.

#### Bootstrap

1. Choose a plugin name. It must be globally unique and must also be the Erlang
   application name.
2. Check out the appropriate `release-5*` branch matching the target EMQX
   version.
3. Either generate the plugin skeleton under `plugins/`:

        cd plugins/
        rebar3 new emqx-plugin {plugin_name}

   or symlink an existing checkout:

        ln -s /path/to/{plugin_name} plugins/{plugin_name}

4. Ensure the skeleton has a `VERSION` file and an `emqx_plugrel` block in
   `rebar.config`. You can use `plugins/emqx_offline_messages/rebar.config` as
   a reference.

#### Build

From the repository root:

    make plugin-{plugin_name}

This runs `rebar3 as emqx_plugrel tar` inside `plugins/{plugin_name}/` and
copies the resulting `.tar.gz` to `_build/plugins/`.

To build every plugin under `plugins/`:

    make plugins

#### Test

Each plugin runs its own Common Test suites via its own rebar3 project:

    make plugins/{plugin_name}-ct

To run CT for every plugin that has a `test/` directory:

    make plugins-ct

> Plugins are self-contained rebar3 projects. Their CT suites do **not** have
> access to in-tree EMQX test helpers (`emqx_common_test_helpers`, `emqx_cth_suite`).
> Either write tests that talk to a running EMQX via the REST API (see
> `plugins/emqx_offline_messages/test/emqx_offline_messages_test_helpers.erl`
> for an example) or test against `emqx_plugin_helper` directly.

#### Dev loop

To rebuild the plugin and hot-install it into the running dev release:

    scripts/run-plugin-dev.sh {plugin_name} [--attach]

This builds the plugin, starts EMQX if needed, copies the tarball into the
plugin install dir, then runs `install`, `enable`, and `start` via
`emqx ctl plugins`.

---

## Packaging and Release

`make plugin-{plugin_name}` produces `_build/plugins/{plugin_name}-{vsn}.tar.gz`.
The tarball contains `release.json`, `{plugin_name}-{vsn}/ebin/`, and the
bundled runtime deps declared in the plugin's `relx` release spec.

Users install the tarball with:

    emqx ctl plugins install {plugin_name}-{vsn}

See the EMQX documentation for full lifecycle management.
