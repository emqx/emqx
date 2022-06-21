#!/usr/bin/env bash

set -euo pipefail
set -x

MAKE_TARGET="${1:-}"

case "${MAKE_TARGET}" in
    emqx-enterprise-*)
        EMQX_NAME='emqx-enterprise'
        ;;
    emqx-*)
        EMQX_NAME='emqx'
        ;;
    *)
        echo "Usage $0 <PKG_TARGET>"
        exit 1
        ;;
esac

case "${MAKE_TARGET}" in
    *-tgz)
        PACKAGE_TYPE='tgz'
        ;;
    *-pkg)
        PACKAGE_TYPE='pkg'
        ;;
    *)
        echo "Unknown package type ${1}"
        exit 2
        ;;
esac

case "${MAKE_TARGET}" in
    *elixir*)
        IS_ELIXIR='yes'
        ;;
    *)
        IS_ELIXIR='no'
        ;;
esac

export DEBUG=1
export CODE_PATH=${CODE_PATH:-"/emqx"}
export SCRIPTS="${CODE_PATH}/scripts"
export EMQX_NAME
export PACKAGE_PATH="${CODE_PATH}/_packages/${EMQX_NAME}"
export RELUP_PACKAGE_PATH="${CODE_PATH}/_upgrade_base"

if [ "$PACKAGE_TYPE" = 'tgz' ]; then
    PKG_SUFFIX="tar.gz"
else
    SYSTEM="$("$SCRIPTS"/get-distro.sh)"
    case "${SYSTEM:-}" in
        ubuntu*|debian*|raspbian*)
            PKG_SUFFIX='deb'
            ;;
        *)
            PKG_SUFFIX='rpm'
            ;;
    esac
fi
PACKAGE_VERSION="$("$CODE_PATH"/pkg-vsn.sh "${EMQX_NAME}")"
PACKAGE_VERSION_LONG="$("$CODE_PATH"/pkg-vsn.sh "${EMQX_NAME}" --long --elixir "${IS_ELIXIR}")"
PACKAGE_NAME="${EMQX_NAME}-${PACKAGE_VERSION_LONG}"
PACKAGE_FILE_NAME="${PACKAGE_NAME}.${PKG_SUFFIX}"

PACKAGE_FILE="${PACKAGE_PATH}/${PACKAGE_FILE_NAME}"
if ! [ -f "$PACKAGE_FILE" ]; then
    echo "$PACKAGE_FILE is not a file"
    exit 1
fi

emqx_prepare(){
    mkdir -p "${PACKAGE_PATH}"

    if [ ! -d "/paho-mqtt-testing" ]; then
        git clone -b develop-4.0 https://github.com/emqx/paho.mqtt.testing.git /paho-mqtt-testing
    fi
    pip3 install pytest
}

emqx_test(){
    cd "${PACKAGE_PATH}"
    local packagename="${PACKAGE_FILE_NAME}"
    case "$PKG_SUFFIX" in
        "tar.gz")
            mkdir -p "${PACKAGE_PATH}/emqx"
            tar -C "${PACKAGE_PATH}/emqx" -zxf "${PACKAGE_PATH}/${packagename}"
            export EMQX_ZONES__DEFAULT__MQTT__SERVER_KEEPALIVE=60
            export EMQX_MQTT__MAX_TOPIC_ALIAS=10
            export EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug
            export EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL=debug
            # if [[ $(arch) == *arm* || $(arch) == aarch64 ]]; then
            #     export EMQX_LISTENERS__QUIC__DEFAULT__ENABLED=false
            # fi
            # sed -i '/emqx_telemetry/d' "${PACKAGE_PATH}"/emqx/data/loaded_plugins

            echo "running ${packagename} start"
            if ! "${PACKAGE_PATH}"/emqx/bin/emqx start; then
                cat "${PACKAGE_PATH}"/emqx/log/erlang.log.1 || true
                cat "${PACKAGE_PATH}"/emqx/log/emqx.log.1 || true
                exit 1
            fi
            IDLE_TIME=0
            while ! curl http://127.0.0.1:18083/api/v5/status >/dev/null 2>&1; do
                if [ $IDLE_TIME -gt 10 ]
                then
                    echo "emqx running error"
                    exit 1
                fi
                sleep 10
                IDLE_TIME=$((IDLE_TIME+1))
            done
            pytest -v /paho-mqtt-testing/interoperability/test_client/V5/test_connect.py::test_basic
            if ! "${PACKAGE_PATH}"/emqx/bin/emqx stop; then
                cat "${PACKAGE_PATH}"/emqx/log/erlang.log.1 || true
                cat "${PACKAGE_PATH}"/emqx/log/emqx.log.1 || true
                exit 1
            fi
            echo "running ${packagename} stop"
            rm -rf "${PACKAGE_PATH}"/emqx
        ;;
        "deb")
            dpkg -i "${PACKAGE_PATH}/${packagename}"
            if [ "$(dpkg -l |grep emqx |awk '{print $1}')" != "ii" ]
            then
                echo "package install error"
                exit 1
            fi

            echo "running ${packagename} start"
            run_test
            echo "running ${packagename} stop"

            dpkg -r "${EMQX_NAME}"
            if [ "$(dpkg -l |grep emqx |awk '{print $1}')" != "rc" ]
            then
                echo "package remove error"
                exit 1
            fi

            dpkg -P "${EMQX_NAME}"
            if dpkg -l |grep -q emqx
            then
                echo "package uninstall error"
                exit 1
            fi
        ;;
        "rpm")
            YUM_RES=$(yum install -y "${PACKAGE_PATH}/${packagename}"| tee /dev/null)
            if [[ $YUM_RES =~ "Failed" ]]; then
               echo "yum install failed"
               exit 1
            fi
            if ! rpm -q "${EMQX_NAME}" | grep -q "${EMQX_NAME}"; then
                echo "package install error"
                exit 1
            fi

            echo "running ${packagename} start"
            run_test
            echo "running ${packagename} stop"

            rpm -e "${EMQX_NAME}"
            if [ "$(rpm -q emqx)" != "package emqx is not installed" ];then
                echo "package uninstall error"
                exit 1
            fi
        ;;
    esac
}

run_test(){
    # sed -i '/emqx_telemetry/d' /var/lib/emqx/loaded_plugins
    emqx_env_vars=$(dirname "$(readlink "$(command -v emqx)")")/../releases/emqx_vars

    if [ -f "$emqx_env_vars" ];
    then
        tee -a "$emqx_env_vars" <<EOF
export EMQX_ZONES__DEFAULT__MQTT__SERVER_KEEPALIVE=60
export EMQX_MQTT__MAX_TOPIC_ALIAS=10
export EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug
export EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL=debug
EOF
        ## for ARM, due to CI env issue, skip start of quic listener for the moment
        # [[ $(arch) == *arm* || $(arch) == aarch64 ]] && tee -a "$emqx_env_vars" <<EOF
# export EMQX_LISTENERS__QUIC__DEFAULT__ENABLED=false
# EOF
    else
        echo "Error: cannot locate emqx_vars"
        exit 1
    fi
    if ! emqx 'start'; then
        cat /var/log/emqx/erlang.log.1 || true
        cat /var/log/emqx/emqx.log.1 || true
        exit 1
    fi
    IDLE_TIME=0
    while ! curl http://127.0.0.1:18083/api/v5/status >/dev/null 2>&1; do
        if [ $IDLE_TIME -gt 10 ]
        then
            echo "emqx running error"
            exit 1
        fi
        sleep 10
        IDLE_TIME=$((IDLE_TIME+1))
    done
    pytest -v /paho-mqtt-testing/interoperability/test_client/V5/test_connect.py::test_basic
    # shellcheck disable=SC2009 # pgrep does not support Extended Regular Expressions
    ps -ef | grep -E '\-progname\s.+emqx\s'
    if ! emqx 'stop'; then
        # shellcheck disable=SC2009 # pgrep does not support Extended Regular Expressions
        ps -ef | grep -E '\-progname\s.+emqx\s'
        echo "ERROR: failed_to_stop_emqx_with_the_stop_command"
        cat /var/log/emqx/erlang.log.1 || true
        cat /var/log/emqx/emqx.log.1 || true
        exit 1
    fi
}

relup_test(){
    if [ ! -d "${RELUP_PACKAGE_PATH}" ]; then
        echo "WARNING: ${RELUP_PACKAGE_PATH} is not a dir, skipped relup test!"
        return 0
    fi
    cd "${RELUP_PACKAGE_PATH}"
    local pattern
    pattern="$EMQX_NAME-$("$CODE_PATH"/pkg-vsn.sh "${EMQX_NAME}" --long --vsn_matcher)"
    while read -r pkg; do
        packagename=$(basename "${pkg}")
        mkdir -p emqx
        tar -C emqx -zxf "$packagename"
        if ! ./emqx/bin/emqx start; then
            cat emqx/log/erlang.log.1 || true
            cat emqx/log/emqx.log.1 || true
            exit 1
        fi
        ./emqx/bin/emqx_ctl status
        ./emqx/bin/emqx versions
        cp "${PACKAGE_PATH}/${PACKAGE_NAME}.tar.gz" ./emqx/releases/
        ./emqx/bin/emqx install "${PACKAGE_VERSION}"
        [ "$(./emqx/bin/emqx versions | grep permanent | awk '{print $2}')" = "${PACKAGE_VERSION}" ] || exit 1
        ./emqx/bin/emqx_ctl status
        ./emqx/bin/emqx stop
        rm -rf emqx
    done < <(find . -maxdepth 1 -name "${pattern}.tar.gz")
}

emqx_prepare
emqx_test
if [ "$IS_ELIXIR" = 'yes' ]; then
    echo "WARNING: skipped relup test for elixir"
else
    relup_test
fi
