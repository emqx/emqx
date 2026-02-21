# EMQX Plugin Development Guide

EMQX is organized as a set of Erlang/OTP applications under the `apps/` directory.
Plugin applications in this monorepo should live under the `plugins/` directory.
In the EMQX monorepo, **Mix** (Elixir build tooling) is used to compile and test all applications together.

There are two project styles to build an EMQX plugin: standalone project, or embedded in the EMQX monorepo.

---

## Build Files Overview

### Standalone Plugin Project

A standalone EMQX plugin is built and released **only with `rebar3`**.

- `rebar.config`
  Used to build the plugin package (`emqx_plugrel`) as a `.tar.gz` artifact.
- `mix.exs`
  **Not required**.

This mode is recommended for independent plugin development outside the EMQX monorepo.

### Plugin Inside the EMQX Monorepo

When a plugin is developed inside the EMQX monorepo, the plugin application should contain:

- `mix.exs`: Required for compile/test/package workflows in the monorepo.
- `VERSION`: Single source of truth for plugin version. `mix.exs` reads this file, and Mix generates the `.app` metadata from `mix.exs`.

> **Note**
> Building a plugin package does **not** automatically load or start the plugin in EMQX.
> Runtime plugin lifecycle is managed explicitly via:
>
> ```bash
> emqx ctl plugins install|enable|start
> ```

---

## Preparation

For plugin development inside this monorepo, package build is driven by Mix via root `make plugin-*` targets.
If you are creating a standalone plugin project outside this monorepo, you still need:

- `rebar3`
- the **emqx-plugin** project template

### Ensure `rebar3`

From the repository root, run:

```bash
make ensure-rebar3
```

This ensures `rebar3` is available locally.

### Install the Plugin Template

Check whether the EMQX plugin template is already installed:

```bash
rebar3 new help
```

If `emqx-plugin (custom)` is **not** listed, install the template manually.

#### Default `rebar3` config location

```bash
mkdir -p ~/.config/rebar3/templates
cd ~/.config/rebar3/templates
git clone https://github.com/emqx/emqx-plugin-template.git
```

#### If `REBAR_CACHE_DIR` is set

```bash
mkdir -p "$REBAR_CACHE_DIR/.config/rebar3/templates"
cd "$REBAR_CACHE_DIR/.config/rebar3/templates"
git clone https://github.com/emqx/emqx-plugin-template.git
```

Verify the installation:

```bash
rebar3 new help
```

---

## Development Modes

With the template installed, there are two common ways to develop an EMQX plugin.

---

## Standalone Plugin Development

For plugins developed outside the EMQX monorepo:

1. Generate a new plugin project:
   ```bash
   rebar3 new emqx-plugin {plugin_name}
   ```

2. Develop and test the plugin using `rebar3`.

3. Build the plugin package following the instructions in:
   https://github.com/emqx/emqx-plugin-template

This mode uses **only `rebar3`** and does not involve Mix or `mix.exs`.

---

## Plugin Development Inside the EMQX Monorepo

This mode is intended for plugin development tightly coupled with a specific EMQX version.

### Bootstrap

1. **Choose a plugin name**
   - Must be globally unique.
   - Must also be the Erlang application name.

2. **Check out the appropriate branch**
   - Use a `release` branch matching the target EMQX version.
   - Example: `release-61` for EMQX 6.1-based development.

3. **Generate the plugin application**
   ```bash
   cd plugins/
   rebar3 new emqx-plugin {plugin_name}
   ```

   You can also keep the plugin in a separate repository and symlink it into `plugins/`.
   Example:
   ```bash
   ln -s /path/to/{plugin_name} plugins/{plugin_name}
   ```

4. **Add `mix.exs` and `VERSION`**
   - Create `plugins/{plugin_name}/mix.exs` so the plugin participates in monorepo build and test workflows.
   - Create `plugins/{plugin_name}/VERSION` and keep plugin version in this single-line file.
   - In `mix.exs`, set `version` from `File.read!("VERSION") |> String.trim()`.
   - Define OTP application metadata in `application/0` of `mix.exs` (for example `mod`, `extra_applications`).
   - You can use `plugins/emqx_username_quota/mix.exs` as a reference.

---

### Development and Testing

- Implement plugin code under:
  ```
  plugins/{plugin_name}/src
  ```

- Add Common Test suites under:
  ```
  plugins/{plugin_name}/test
  ```

- Run Common Test for the plugin only:
  ```bash
  make plugins/{plugin_name}-ct
  ```

Example:

```bash
make plugins/emqx_username_quota-ct
```

---

### Integration Testing and Packaging

- For quick local integration testing (without adding the plugin to EMQX boot applications):
  ```bash
  scripts/run-plugin-dev.sh {plugin_name} [--attach]
  ```

- Build the plugin package from the repository root (Mix-driven packaging):
  ```bash
  make plugin-{plugin_name}
  ```

This produces a `.tar.gz` plugin artifact under `_build/plugins/`, suitable for installation via `emqx ctl plugins`.

---

### Example

- **Plugin name:** `emqx_username_quota`
- **Application path:** `plugins/emqx_username_quota`
- **Package build command:**
  ```bash
  make plugin-emqx_username_quota
  ```
