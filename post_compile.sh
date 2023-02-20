#!/bin/sh -eu

case ${REBAR_BUILD_DIR} in *+test) exit 0;; esac

if [ ! -d ${REBAR_BUILD_DIR} ]; then
    # profile dir is not created yet
    exit 0
fi

cd ${REBAR_BUILD_DIR}

## Collect config files, some are direct usable
## some are relx-overlay templated
rm -rf conf
mkdir -p conf/plugins
for conf in lib/*/etc/*.config ; do
    if [ "emqx.conf" = "${conf##*/}" ]; then
        cp ${conf} conf/
    elif [ "acl.conf" = "${conf##*/}" ]; then
        cp ${conf} conf/
    elif [ "ssl_dist.conf" = "${conf##*/}" ]; then
        cp ${conf} conf/
    else
        cp ${conf} conf/plugins/
    fi
done

