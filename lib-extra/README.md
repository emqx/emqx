# EMQ X Extra plugin apps

This directory keeps a `plugins` file which defines all the approved
external plugins from open-source community.

The (maybe broken) symlinks are keept to help testing plugins
in this umbrella project.

## How to build `plugin_foo`

Add `plugin_foo` as a rebar3 dependency in `plugins` file.

e.g.

```
{erlang_plugins,
  [ {plugin_foo, {git, "https://github.com/bar/plugin-foo.git", {tag, "0.1.0"}}}
  ]
}.
```

Exeucte command

```
export EMQX_EXTRA_PLUGINS='plugin_foo'
make
```

The plugin source code should downloaded to `_build/default/lib/plugin_foo`

NOTE: Shallow clone with depth=1 is used for git dependencies.

## How to test `plugin_foo`

If the source code in `_build` is already symlinked from `lib-extra/`,
you may directlly run tests with commands below.

```bash
./rebar3 eunit --dir lib-extra/plugin_foo
./rebar3 ct --dir lib-extra/plugin_foo
```

In case the plugin is being actively developed
it can be cloned to `lib-extra`, e.g. `lib-extra/plugin-bar-dev`
then it can be tested with commands below:

```bash
./rebar3 eunit --dir lib-extra/plugin-bar-dev
./rebar3 ct --dir lib-extra/plugin-bar-dev
```
