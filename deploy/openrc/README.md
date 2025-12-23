# EMQX OpenRC Init Script

This directory contains the OpenRC init script for running EMQX as a system service on Alpine Linux and Gentoo.

Disclaimer: currently this script is not covered by CI.

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

The init script uses `supervise-daemon` which handles graceful shutdown automatically. You can configure the stop timeout in `/etc/conf.d/emqx` if needed.

### Process supervision

Using `supervise-daemon` eliminates the PID file tracking issues described in [issue #4640](https://github.com/emqx/emqx/issues/4640) because OpenRC directly monitors the EMQX foreground process.

## Technical Details

### How It Works

The init script uses OpenRC's `supervise-daemon` to run EMQX in foreground mode (matching the systemd approach):

1. **Start**: Launches EMQX with `emqx foreground` command
2. **Stop**: `supervise-daemon` handles graceful shutdown with configurable timeout
3. **Monitoring**: OpenRC automatically monitors the process and can restart it if it crashes

This approach provides:
- Better process supervision compared to traditional daemon mode
- Consistent behavior with systemd units
- No PID file tracking issues
- Automatic process monitoring and restart capabilities

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
