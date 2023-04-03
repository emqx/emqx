# emqx_ctl

This application accepts dynamic `emqx ctl` command registrations so plugins can add their own commands.
Please note that the 'proxy' command `emqx_ctl` is considered deprecated, going forward, please use `emqx ctl` instead.

## Add a new command

To add a new command, the application must implement a callback function to handle the command, and register the command with `emqx_ctl:register_command/2` API.

### Register

To add a new command which can be executed from `emqx ctl`, the application must call `emqx_ctl:register_command/2` API to register the command.

For example, to add a new command `myplugin` which is to be executed as `emqx ctl myplugin`, the application must call `emqx_ctl:register_command/2` API as follows:

```erlang
emqx_ctl:register_command(mypluin, {myplugin_cli, cmd}).
```

### Callback

The callback function must be exported by the application and must have the following signature:

```erlang
cmd([Arg1, Arg2, ...]) -> ok.
```

It must also implement a special clause to handle the `usage` argument:

```erlang
cmd([usage]) -> "myplugin [arg1] [arg2] ...";
```

### Utility

The `emqx_ctl` application provides some utility functions which help to format the output of the command.
For example `emqx_ctl:print/2` and `emqx_ctl:usage/1`.

## Reference

[emqx_management_cli](../emqx_management/src/emqx_mgmt_cli.erl) can be taken as a reference for how to implement a command.
