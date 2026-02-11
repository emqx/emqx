# EMQX plugins

Starting from EMQX 5.0, plugins are developed in independent projects.

This is different from EMQX 4.3 (and later 4.x releases) for which the plugins have to
be developed inside the emqx.git umbrella project.

## Build a plugin as a standalone project

Use the rebar3 template: [emqx-plugin-template](https://github.com/emqx/emqx-plugin-template).

## Build a plugin in this monorepo (`contribute-*` branches)

- Pick a plugin name. The name should be globally unique enough and will also be the Erlang app name.
- Check out a `contribute-*` branch. For example, use `contribute-61` for EMQX 6.1-based plugin work.
- Generate the app with the template:

   `rebar3 new emqx-plugin {plugin_name}`

- Place the generated plugin app under `apps/{plugin_name}`.
- Add `apps/{plugin_name}/mix.exs` so the app is included in monorepo Mix build/test workflows.
- Implement plugin code in `apps/{plugin_name}/src`.
- Add tests in `apps/{plugin_name}/test`.
- Run tests like other native apps in this repo.
- Build the plugin package from repo root:

   `make plugin-{plugin_name}`

### Example

For the example plugin planned in this branch:

- plugin name: `emqx_username_quota`
- app path: `apps/emqx_username_quota`
- package build command: `make plugin-emqx_username_quota`
