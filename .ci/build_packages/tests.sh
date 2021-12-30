#!/usr/bin/env bash

## This script tests built package start/stop
## Accept 2 args PACKAGE_NAME and PACKAGE_TYPE

set -x -e -u

if [ -z "${1:-}" ]; then
    echo "Usage $0 <PACKAGE_NAME> zip|pkg"
    exit 1
fi

if [ "${2:-}" != 'zip' ] && [ "${2:-}" != 'pkg' ]; then
    echo "Usage $0 <PACKAGE_NAME> zip|pkg"
    exit 1
fi

PACKAGE_NAME="${1}"
PACKAGE_TYPE="${2}"

export CODE_PATH=${CODE_PATH:-"/emqx"}
export EMQX_NAME=${EMQX_NAME:-"emqx"}
export PACKAGE_PATH="${CODE_PATH}/_packages/${EMQX_NAME}"
export RELUP_PACKAGE_PATH="${CODE_PATH}/_upgrade_base"
# export EMQX_NODE_NAME="emqx-on-$(uname -m)@127.0.0.1"
# export EMQX_NODE_COOKIE=$(date +%s%N)

if [ "$PACKAGE_TYPE" = 'zip' ]; then
    PKG_SUFFIX="zip"
else
    SYSTEM="$($CODE_PATH/scripts/get-distro.sh)"
    case "${SYSTEM:-}" in
        ubuntu*|debian*|raspbian*)
            PKG_SUFFIX='deb'
            ;;
        *)
            PKG_SUFFIX='rpm'
            ;;
    esac
fi
PACKAGE_FILE_NAME="${PACKAGE_NAME}.${PKG_SUFFIX}"

PACKAGE_FILE="${PACKAGE_PATH}/${PACKAGE_FILE_NAME}.${PKG_SUFFIX}"
if ! [ -f "$PACKAGE_FILE" ]; then
    echo "$PACKAGE_FILE is not a file"
fi

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
    local packagename="${PACKAGE_FILE_NAME}"

        case ${packagename##*.} in
            "zip")
                unzip -q "${PACKAGE_PATH}/${packagename}"
                export EMQX_ZONE__EXTERNAL__SERVER__KEEPALIVE=60 \
                       EMQX_MQTT__MAX_TOPIC_ALIAS=10
                sed -i '/emqx_telemetry/d' "${PACKAGE_PATH}"/emqx/data/loaded_plugins

                echo "running ${packagename} start"
                if ! "${PACKAGE_PATH}"/emqx/bin/emqx start; then
                    cat "${PACKAGE_PATH}"/emqx/log/erlang.log.1 || true
                    cat "${PACKAGE_PATH}"/emqx/log/emqx.log.1 || true
                    exit 1
                fi
                IDLE_TIME=0
                while ! "${PACKAGE_PATH}"/emqx/bin/emqx_ctl status | grep -qE 'Node\s.*@.*\sis\sstarted'
                do
                    if [ $IDLE_TIME -gt 10 ]
                    then
                        echo "emqx running error"
                        exit 1
                    fi
                    sleep 10
                    IDLE_TIME=$((IDLE_TIME+1))
                done
                pytest -v /paho-mqtt-testing/interoperability/test_client/V5/test_connect.py::test_basic
                "${PACKAGE_PATH}"/emqx/bin/emqx stop
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
                running_test
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
                if [[ "${ARCH}" == "amd64" && $(rpm -E '%{rhel}') == 7 ]] ; then
                    # EMQX OTP requires openssl11 to have TLS1.3 support
                    yum install -y openssl11
                fi

                rpm -ivh "${PACKAGE_PATH}/${packagename}"
                if ! rpm -q emqx | grep -q emqx; then
                    echo "package install error"
                    exit 1
                fi

                echo "running ${packagename} start"
                running_test
                echo "running ${packagename} stop"

                rpm -e "${EMQX_NAME}"
                if [ "$(rpm -q emqx)" != "package emqx is not installed" ];then
                    echo "package uninstall error"
                    exit 1
                fi
            ;;

        esac
}

running_test(){
    export EMQX_ZONE__EXTERNAL__SERVER__KEEPALIVE=60 \
           EMQX_MQTT__MAX_TOPIC_ALIAS=10
    sed -i '/emqx_telemetry/d' /var/lib/emqx/loaded_plugins

    if ! emqx start; then
        cat /var/log/emqx/erlang.log.1 || true
        cat /var/log/emqx/emqx.log.1 || true
        exit 1
    fi
    IDLE_TIME=0
    while ! emqx_ctl status | grep -qE 'Node\s.*@.*\sis\sstarted'
    do
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
    emqx stop || kill "$(ps -ef | grep -E '\-progname\s.+emqx\s' |awk '{print $2}')"
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
