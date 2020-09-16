# emqx_extension_hook

The `emqx_extension_hook` extremly enhance the extensibility for EMQ X. It allow using an others programming language to mount the hooks intead of erlang.

## Feature

- [x] Support `python` and `java`.
- [x] Support all hooks of emqx.
- [x] Allows you to use the return value to extend emqx behavior.

We temporarily no plans to support other languages. Plaease open a issue if you have to use other programming languages.

## Architecture

```
 EMQ X                                      Third-party Runtimes
+========================+                 +====================+
|    Extension           |                 |                    |
|   +----------------+   |     Hooks       |  Python scripts /  |
|   |    Drivers     | ------------------> |  Java Classes   /  |
|   +----------------+   |     (pipe)      |  Others ...        |
|                        |                 |                    |
+========================+                 +====================+
```

## Drivers

### Python

***Requirements:***

- It requires the emqx hosted machine has Python3 Runtimes (not support python2)
- The `python3` executable commands in your shell

***Examples:***

See `test/scripts/main.py`

### Java

***Requirements:***

- It requires the emqx hosted machine has Java 8+ Runtimes
- An executable commands in your shell, i,g: `java`

***Examples:***

See `test/scripts/Main.java`

## Configurations

| Name                | Data Type | Options                               | Default          | Description                      |
| ------------------- | --------- | ------------------------------------- | ---------------- | -------------------------------- |
| drivers             | Enum      | `python3`<br />`java`                 | `python3`        | Drivers type                     |
| <type>.path         | String    | -                                     | `data/extension` | The codes/library search path    |
| <type>.call_timeout | Duration  | -                                     | `5s`             | Function call timeout            |
| <type>.pool_size    | Integer   | -                                     | `8`              | The pool size for the driver     |
| <type>.init_module  | String    | -                                     | main             | The module name for initial call |

## SDK

See `sdk/README.md`

## Known Issues or TODOs

**Configurable Log System**

- use stderr to print logs to the emqx console instead of stdout. An alternative is to print the logs to a file.
- The Java driver can not redirect the `stderr` stream to erlang vm on Windows platform.

## Reference

- [erlport](https://github.com/hdima/erlport)
- [Eexternal Term Format](http://erlang.org/doc/apps/erts/erl_ext_dist.html)
- [The Ports Tutorial of Erlang](http://erlang.org/doc/tutorial/c_port.html)
