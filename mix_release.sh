#!/bin/bash

set -ex

mix release --overwrite

## FIXME: temporary hacks to get the needed configurations for the
## release to properly start.

## Assumes that `make emqx` has been run before the mix build to
## generate the correct configs.

mkdir -p _build/dev/rel/emqx/data/configs/
LATEST_APP_CONFIG=$(ls -rt _build/emqx/rel/emqx/data/configs/app*.config | tail -n 1)

# FIXME!
cp ${LATEST_APP_CONFIG} _build/dev/rel/emqx/releases/5.0.0-beta.2/sys.config
sed -i -E 's#_build/emqx/rel/emqx/etc/emqx.conf#_build/dev/rel/emqx/etc/emqx.conf#g' _build/dev/rel/emqx/releases/5.0.0-beta.2/sys.config
sed -i -E 's#logger_level,warning#logger_level,debug#g' _build/dev/rel/emqx/releases/5.0.0-beta.2/sys.config
sed -i -E 's#level => warning#level => debug#g' _build/dev/rel/emqx/releases/5.0.0-beta.2/sys.config


# cp _build/emqx/rel/emqx/releases/emqx_vars _build/dev/rel/emqx/releases/
cp _build/emqx/rel/emqx/etc/emqx.conf _build/dev/rel/emqx/etc/
cp -r apps/emqx/etc/certs _build/dev/rel/emqx/etc/

echo "telemetry { enable = false }" >> _build/dev/rel/emqx/etc/emqx.conf
