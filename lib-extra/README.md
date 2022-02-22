# EMQX Extra plugin apps

This directory keeps a `plugins` file which defines all the approved
external plugins from open-source community.

The (maybe broken) symlinks are keept to help testing plugins
in this umbrella project.

## Add a plugin to the project

Add `plugin_foo` as a rebar3 dependency in `plugins` file.

e.g. For an Erlang plugin named `plugin_foo`:

```
{erlang_plugins,
  [ {plugin_foo, {git, "https://github.com/bar/plugin-foo.git", {tag, "0.1.0"}}}
  ]
}.
```

Note: The `-emqx_plugin(?MODULE)` attribute should be added to
`<plugin-name>_app.erl` file to indicate that this is an EMQX Broker plugin.

For example:
```erlang
%% plugin_foo_app.erl
-emqx_plugin(?MODULE)
```

## Build a release

```
$ export EMQX_EXTRA_PLUGINS=plugin_foo,plugin_bar
$ make
```

If all goes as expected, there should be two directories in the release:

```
_build/emqx/rel/emqx/lib/plugin_foo-<..vsn..>/
```

## Run your code

Start the node (interactive mode)

```
./_build/emqx/rel/emqx/bin/emqx console
```

Load the plugin with command:

```
./_build/emqx/rel/emqx/bin/emqx_ctl plugins load plugin_foo
```

## Test the plugin

If the plugin is added as a rebar dependency, it should be cloned
to `_build/default/lib/plugin_foo`.

Before you can test it, you need to make sure there is a symlink
in `lib-extra` pointing to the clone. For instance, the `emqx_plugin_template`
plugin is linked like this

`emqx_plugin_template -> ../_build/default/lib/emqx_plugin_template/`

To run its teste cases:

```bash
./rebar3 eunit --dir lib-extra/plugin_foo
mkae lib-extra/plugin_foo-ct
```

NOTE: we should `depth=1` shallow clone into `_build/` directory,
for plugins being actively developed, you can place the clone in `lib-extra/`

## Caveats

* Elixir dependency in Erlang is not quite nicely supported as incremental builds,
  meaning you will not be able to edit the code in this project and get recompiled
  in the next `make` command.

* To have the plugin enabled/loaded by default, you can include it in the template
  `data/loaded_plugins.tmpl`
