ARG PLUGIN_VSN=0.1.0
FROM emqx/emqx:6.1.1

ARG PLUGIN_VSN
ENV PLUGIN_NAME_VSN=emqx_bridge_mqtt_dq-${PLUGIN_VSN}

COPY --chown=emqx:emqx _build/plugins/${PLUGIN_NAME_VSN}.tar.gz /opt/emqx/plugins/${PLUGIN_NAME_VSN}.tar.gz

RUN cd /opt/emqx/plugins && \
    mkdir -p ${PLUGIN_NAME_VSN} && \
    tar zxf ${PLUGIN_NAME_VSN}.tar.gz -C ${PLUGIN_NAME_VSN}

RUN cat <<EOF >> /opt/emqx/etc/base.hocon
plugins {
    states = [
        {
            name_vsn = "${PLUGIN_NAME_VSN}"
            enable = true
        }
    ]
}
EOF
