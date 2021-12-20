#!/bin/bash
set -x -e -u
export DEBUG=1
export CODE_PATH=${CODE_PATH:-"/emqx"}
export EMQX_NAME=${EMQX_NAME:-"emqx"}
export PACKAGE_PATH="${CODE_PATH}/_packages/${EMQX_NAME}"
export RELUP_PACKAGE_PATH="${CODE_PATH}/_upgrade_base"

case "$(uname -m)" in
    x86_64)
        ARCH='amd64'
        ;;
    aarch64)
        ARCH='arm64'
        ;;
    arm*)
        ARCH=arm
        ;;
esac
export ARCH

emqx_prepare(){
    mkdir -p "${PACKAGE_PATH}"

    if [ ! -d "/paho-mqtt-testing" ]; then
        git clone -b develop-4.0 https://github.com/emqx/paho.mqtt.testing.git /paho-mqtt-testing
    fi
    pip3 install pytest
}

emqx_test(){
    cd "${PACKAGE_PATH}"

    for var in "$PACKAGE_PATH"/"${EMQX_NAME}"-*;do
        case ${var##*.} in
            "zip")
                packagename=$(basename "${PACKAGE_PATH}/${EMQX_NAME}"-*.zip)
                unzip -q "${PACKAGE_PATH}/${packagename}"
                export EMQX_ZONES__DEFAULT__MQTT__SERVER_KEEPALIVE=60
                export EMQX_MQTT__MAX_TOPIC_ALIAS=10
                export EMQX_LOG__CONSOLE_HANDLER__LEVEL=debug
                export EMQX_LOG__FILE_HANDLERS__DEFAULT__LEVEL=debug
                if [[ $(arch) == *arm* || $(arch) == aarch64 ]]; then
                    export EMQX_LISTENERS__QUIC__DEFAULT__ENABLED=false
                    export WAIT_FOR_ERLANG_STOP=120
                fi
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
                packagename=$(basename "${PACKAGE_PATH}/${EMQX_NAME}"-*.deb)
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
                packagename=$(basename "${PACKAGE_PATH}/${EMQX_NAME}"-*.rpm)

                if [[ "${ARCH}" == "amd64" && $(rpm -E '%{rhel}') == 7 ]] ;
                then
                    # EMQX OTP requires openssl11 to have TLS1.3 support
                    yum install -y openssl11;
                fi
                rpm -ivh "${PACKAGE_PATH}/${packagename}"
                if ! rpm -q emqx | grep -q emqx; then
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
    done
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
        [[ $(arch) == *arm* || $(arch) == aarch64 ]] && tee -a "$emqx_env_vars" <<EOF
export EMQX_LISTENERS__QUIC__DEFAULT__ENABLED=false
export WAIT_FOR_ERLANG_STOP=120
EOF
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
    TARGET_VERSION="$("$CODE_PATH"/pkg-vsn.sh)"
    if [ -d "${RELUP_PACKAGE_PATH}" ];then
        cd "${RELUP_PACKAGE_PATH}"

        find . -maxdepth 1 -name "${EMQX_NAME}-*-${ARCH}.zip" |
            while read -r pkg; do
                packagename=$(basename "${pkg}")
                unzip "$packagename"
                if ! ./emqx/bin/emqx start; then
                    cat emqx/log/erlang.log.1 || true
                    cat emqx/log/emqx.log.1 || true
                    exit 1
                fi
                ./emqx/bin/emqx_ctl status
                ./emqx/bin/emqx versions
                cp "${PACKAGE_PATH}/${EMQX_NAME}"-*-"${TARGET_VERSION}-${ARCH}".zip ./emqx/releases
                ./emqx/bin/emqx install "${TARGET_VERSION}"
                [ "$(./emqx/bin/emqx versions |grep permanent | awk '{print $2}')" = "${TARGET_VERSION}" ] || exit 1
                ./emqx/bin/emqx_ctl status
                ./emqx/bin/emqx stop
                rm -rf emqx
            done
   fi
}

emqx_prepare
emqx_test
relup_test
