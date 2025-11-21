# EMQX OpenRC Init Script for Alpine Linux

This directory contains the OpenRC init script for running EMQX as a system service on Alpine Linux.

## Files

- `emqx.initd` - OpenRC init script
- `emqx.confd` - Configuration file with default settings
- `README.md` - This file

## Prerequisites

1. EMQX must be installed on your system (typically in `/usr/lib/emqx`)
2. The `emqx` user and group should exist:
   ```sh
   addgroup -S emqx
   adduser -S -D -h /var/lib/emqx -s /sbin/nologin -G emqx emqx
   ```

3. Required directories:
   ```sh
   mkdir -p /var/lib/emqx /var/log/emqx /var/run/emqx
   chown -R emqx:emqx /var/lib/emqx /var/log/emqx /var/run/emqx
   ```

## Installation

1. Copy the init script to `/etc/init.d/`:
   ```sh
   cp emqx.initd /etc/init.d/emqx
   chmod +x /etc/init.d/emqx
   ```

2. Copy the configuration file to `/etc/conf.d/`:
   ```sh
   cp emqx.confd /etc/conf.d/emqx
   ```

3. (Optional) Edit `/etc/conf.d/emqx` to customize settings if needed

4. Add EMQX to the default runlevel:
   ```sh
   rc-update add emqx default
   ```

## Usage

### Start EMQX
```sh
rc-service emqx start
```

### Stop EMQX
```sh
rc-service emqx stop
```

### Restart EMQX
```sh
rc-service emqx restart
```

### Check Status
```sh
rc-service emqx status
```

### Reload Configuration
```sh
rc-service emqx reload
```
Note: EMQX configuration is typically updated via the dashboard or HTTP API. This command provides information about configuration management.

## Configuration

Edit `/etc/conf.d/emqx` to customize:

- `EMQX_USER` - User to run EMQX as (default: emqx)
- `EMQX_GROUP` - Group to run EMQX as (default: emqx)
- `EMQX_HOME` - EMQX installation directory (default: /usr/lib/emqx)
- `EMQX_BIN` - EMQX binary location (default: /usr/bin/emqx)
- `EMQX_PIDFILE` - PID file location (default: /var/run/emqx/emqx.pid)
- `EMQX_LOG_DIR` - Log directory (default: /var/log/emqx)

## Troubleshooting

### Service fails to start

1. Check logs in `/var/log/emqx/`
2. Verify EMQX binary exists and is executable:
   ```sh
   ls -la /usr/bin/emqx
   ```
3. Ensure required directories exist and have correct permissions:
   ```sh
   ls -la /var/lib/emqx /var/log/emqx /var/run/emqx
   ```

### Service fails to stop properly

The init script uses a graceful shutdown approach:
1. First attempts `emqx stop` command (waits up to 30 seconds)
2. If still running, uses `start-stop-daemon` with SIGTERM (waits 30 seconds)
3. Finally uses SIGKILL if needed (waits 5 seconds)

Total timeout is approximately 65 seconds.

### PID file issues

The init script monitors the `run_erl` process (EMQX's daemon wrapper) rather than relying solely on PID files. This resolves the PID mismatch issues described in [issue #4640](https://github.com/emqx/emqx/issues/4640).

## Technical Details

### How It Works

EMQX runs in daemon mode using Erlang's `run_erl` wrapper. The init script:

1. **Start**: Launches EMQX with `emqx start`, then locates and monitors the `run_erl` process
2. **Stop**: Issues graceful shutdown via `emqx stop`, then forcefully terminates `run_erl` if needed
3. **Status**: Uses EMQX's built-in `emqx status` command

This approach ensures reliable service management by tracking the actual daemon process rather than relying on potentially stale PID files.

### Dependencies

The service depends on:
- `net` - Network services must be running
- `dns` - DNS resolution (optional but recommended)
- `logger` - System logger (optional but recommended)

Starts after `firewall` if present.

## References

- Original issue: https://github.com/emqx/emqx/issues/4640
- Related discussion: https://github.com/emqx/emqx/discussions/4622
- OpenRC documentation: https://github.com/OpenRC/openrc
- EMQX documentation: https://www.emqx.io/docs/

## License

Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.

Licensed under the Apache License, Version 2.0.
