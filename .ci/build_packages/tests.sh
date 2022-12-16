#!/usr/bin/env bash

## This script tests built package start/stop
## Accept 2 args PROFILE and PACKAGE_TYPE

set -x -e -u

if [ -z "${1:-}" ]; then
    echo "Usage $0 <PROFILE>  e.g. emqx, emqx-edge"
    exit 1
fi

if [ "${2:-}" != 'zip' ] && [ "${2:-}" != 'pkg' ]; then
    echo "Usage $0 <PACKAGE_NAME> zip|pkg"
    exit 1
fi

PROFILE="${1}"
PACKAGE_TYPE="${2}"

export CODE_PATH=${CODE_PATH:-"/emqx"}
export PACKAGE_PATH="${CODE_PATH}/_packages/${PROFILE}"
export RELUP_PACKAGE_PATH="${CODE_PATH}/_upgrade_base"
# export EMQX_NODE_NAME="emqx-on-$(uname -m)@127.0.0.1"
# export EMQX_NODE_COOKIE=$(date +%s%N)

SYSTEM="$("$CODE_PATH"/scripts/get-distro.sh)"

if [ "$PACKAGE_TYPE" = 'zip' ]; then
    PKG_SUFFIX="zip"
else
    case "${SYSTEM:-}" in
        ubuntu*|debian*|raspbian*)
            PKG_SUFFIX='deb'
            ;;
        *)
            PKG_SUFFIX='rpm'
            ;;
    esac
fi

PACKAGE_NAME="${PROFILE}-$("$CODE_PATH"/scripts/pkg-full-vsn.sh)"
OLD_PACKAGE_PATTERN="${PROFILE}-$("$CODE_PATH"/scripts/pkg-full-vsn.sh 'vsn_matcher')"
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

            dpkg -r "${PROFILE}"
            if [ "$(dpkg -l |grep emqx |awk '{print $1}')" != "rc" ]
            then
                echo "package remove error"
                exit 1
            fi

            dpkg -P "${PROFILE}"
            if dpkg -l |grep -q emqx
            then
                echo "package uninstall error"
                exit 1
            fi
        ;;
        "rpm")
            # yum wants python2
            if [[ "${SYSTEM:-}" == "amzn2" ]]; then
                alternatives --list | grep python && alternatives --set python /usr/bin/python2
            fi
            YUM_RES="$(yum install -y "${PACKAGE_PATH}/${packagename}"| tee /dev/null)"
            if [[ $YUM_RES =~ "Failed" ]]; then
               echo "yum install failed"
               exit 1
            fi
            # restore python3
            if [[ "${SYSTEM:-}" == "amzn2" ]]; then
                alternatives --list | grep python && alternatives --set python /usr/bin/python3
            fi

            if ! rpm -q "${PROFILE}" | grep -q "${PROFILE}"; then
                echo "package install error"
                exit 1
            fi

            echo "running ${packagename} start"
            running_test
            echo "running ${packagename} stop"

            rpm -e "${PROFILE}"
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
    if [ ! -d "${RELUP_PACKAGE_PATH}" ];then
        return 0
    fi
    cd "${RELUP_PACKAGE_PATH}"
    while read -r pkg; do
        packagename=$(basename "${pkg}")
        unzip -q "$packagename"
        if ! ./emqx/bin/emqx start; then
            cat emqx/log/erlang.log.1 || true
            cat emqx/log/emqx.log.1 || true
            exit 1
        fi
        ./emqx/bin/emqx_ctl status
        ./emqx/bin/emqx versions
        OldVsn="$(./emqx/bin/emqx eval 'Versions=[{S, V} ||  {_,V,_, S} <- release_handler:which_releases()],
                    Current = proplists:get_value(current, Versions, proplists:get_value(permanent, Versions)),
                    io:format("~s", [Current])')"
        cp "${PACKAGE_PATH}/${PROFILE}-${TARGET_VERSION}"-*.zip ./emqx/releases/
        ./emqx/bin/emqx install "${TARGET_VERSION}"
        [ "$(./emqx/bin/emqx versions |grep permanent | awk '{print $2}')" = "${TARGET_VERSION}" ] || exit 1
        export EMQX_WAIT_FOR_STOP=300
        ./emqx/bin/emqx_ctl status

        # also test remove old rel
        ./emqx/bin/emqx uninstall "$OldVsn"

        # check emqx still runs
        ./emqx/bin/emqx ping

        if ! ./emqx/bin/emqx stop; then
            cat emqx/log/erlang.log.1 || true
            cat emqx/log/emqx.log.1 || true
            echo "failed to stop emqx"
            exit 1
        fi
        rm -rf emqx
    done < <(find . -maxdepth 1 -name "${OLD_PACKAGE_PATTERN}.zip")
}

emqx_prepare
emqx_test
relup_test
