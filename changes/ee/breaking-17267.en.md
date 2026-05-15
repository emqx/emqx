The `node.max_ports` config now defaults to `auto`, which scales the Erlang VM port limit (`+Q`) with the number of logical CPU cores: 64000 ports per core for up to 8 cores, and 1048576 (the historical fixed default) above that. Explicit integer values are still accepted.

This is a behavior change for nodes upgraded from earlier versions where `max_ports` defaulted to a fixed 1048576: hosts with 8 or fewer CPU cores will now boot with a smaller port table. Setups that rely on accepting more than `cores * 64000` connections must set `node.max_ports` explicitly (and restart the node) before upgrading.

The hidden `node.process_limit` setting is reinstated as an override: when set to a value larger than the derived limit (`2 * max_ports`), it is respected; smaller values are ignored so the process table never under-sizes the port table.
