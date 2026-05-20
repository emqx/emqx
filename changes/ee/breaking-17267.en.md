The `node.max_ports` config now defaults to `auto`, which scales the Erlang VM port limit (`+Q`) with the number of logical CPU cores: 65536 ports per core for up to 8 cores, and 1048576 (the historical fixed default) above that. Explicit integer values are still accepted.

This is a behavior change for nodes upgraded from earlier versions where `max_ports` defaulted to a fixed 1048576: hosts with 8 or fewer CPU cores will now boot with a smaller port table. Setups that rely on accepting more than `cores * 65536` connections must set `node.max_ports` explicitly (and restart the node) before upgrading.

The hidden `node.process_limit` setting is reinstated as an override: when set to a value larger than the derived limit (`2 * max_ports`), it is respected; smaller values are ignored so the process table never under-sizes the port table.

A new `node.schedulers` setting (default `auto`) controls the Erlang scheduler count (`+S`). With `auto`, the count is capped at the number of logical processors actually available to the VM (`sched_getaffinity` on Linux), so containers limited via `--cpuset-cpus` or Kubernetes CPU requests no longer spawn scheduler OS threads they cannot run in parallel. Set it to a positive integer to override the auto-detected value.
