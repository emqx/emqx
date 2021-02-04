#!/bin/sh
set -x -e -u
export EMQX_NAME=${EMQX_NAME:-"emqx"}
export PACKAGE_PATH="/emqx/_packages/${EMQX_NAME}"
export RELUP_PACKAGE_PATH="/emqx/relup_packages/${EMQX_NAME}"
# export EMQX_NODE_NAME="emqx-on-$(uname -m)@127.0.0.1"
# export EMQX_NODE_COOKIE=$(date +%s%N)

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
                sed -i "/zone.external.server_keepalive/c zone.external.server_keepalive = 60" "${PACKAGE_PATH}"/emqx/etc/emqx.conf
                sed -i "/mqtt.max_topic_alias/c mqtt.max_topic_alias = 10" "${PACKAGE_PATH}"/emqx/etc/emqx.conf
                sed -i '/emqx_telemetry/d' "${PACKAGE_PATH}"/emqx/data/loaded_plugins

                if echo "${EMQX_DEPS_DEFAULT_VSN#v}" | grep -qE "[0-9]+\.[0-9]+(\.[0-9]+)?-(alpha|beta|rc)\.[0-9]"; then
                    if [ ! -d "${PACKAGE_PATH}/emqx/lib/emqx-${EMQX_DEPS_DEFAULT_VSN#v}" ] || [ ! -d "${PACKAGE_PATH}/emqx/releases/${EMQX_DEPS_DEFAULT_VSN#v}" ] ;then
                        echo "emqx zip version error"
                        exit 1
                    fi
                fi

                echo "running ${packagename} start"
                "${PACKAGE_PATH}"/emqx/bin/emqx start || tail "${PACKAGE_PATH}"/emqx/log/erlang.log.1
                IDLE_TIME=0
                while [ -z "$("${PACKAGE_PATH}"/emqx/bin/emqx_ctl status |grep 'is running'|awk '{print $1}')" ]
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
                packagename=$(basename "${PACKAGE_PATH}/${EMQX_NAME}"-*.deb)
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
                packagename=$(basename "${PACKAGE_PATH}/${EMQX_NAME}"-*.rpm)
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
    done
}

running_test(){
    if echo "${EMQX_DEPS_DEFAULT_VSN#v}" | grep -qE "[0-9]+\.[0-9]+(\.[0-9]+)?-(alpha|beta|rc)\.[0-9]"; then
        if [ ! -d /usr/lib/emqx/lib/emqx-"${EMQX_DEPS_DEFAULT_VSN#v}" ] || [ ! -d /usr/lib/emqx/releases/"${EMQX_DEPS_DEFAULT_VSN#v}" ];then
            echo "emqx package version error"
            exit 1
        fi
    fi

    sed -i "/zone.external.server_keepalive/c zone.external.server_keepalive = 60" /etc/emqx/emqx.conf
    sed -i "/mqtt.max_topic_alias/c mqtt.max_topic_alias = 10" /etc/emqx/emqx.conf
    sed -i '/emqx_telemetry/d' /var/lib/emqx/loaded_plugins

    emqx start || tail /var/log/emqx/erlang.log.1
    IDLE_TIME=0
    while [ -z "$(emqx_ctl status |grep 'is running'|awk '{print $1}')" ]
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

    if [ "$(sed -n '/^ID=/p' /etc/os-release | sed -r 's/ID=(.*)/\1/g' | sed 's/"//g')" = ubuntu ] \
    || [ "$(sed -n '/^ID=/p' /etc/os-release | sed -r 's/ID=(.*)/\1/g' | sed 's/"//g')" = debian ] \
    || [ "$(sed -n '/^ID=/p' /etc/os-release | sed -r 's/ID=(.*)/\1/g' | sed 's/"//g')" = raspbian ];then
        service emqx start || tail /var/log/emqx/erlang.log.1
        IDLE_TIME=0
        while [ -z "$(emqx_ctl status |grep 'is running'|awk '{print $1}')" ]
        do
            if [ $IDLE_TIME -gt 10 ]
            then
                echo "emqx service error"
                exit 1
            fi
            sleep 10
            IDLE_TIME=$((IDLE_TIME+1))
        done
        service emqx stop
    fi
}

relup_test(){
    if [ -d "${RELUP_PACKAGE_PATH}" ];then
        cd "${RELUP_PACKAGE_PATH }"

        for var in "${EMQX_NAME}"-*-"$(uname -m)".zip;do
            packagename=$(basename "${var}")
            unzip "$packagename"
            ./emqx/bin/emqx start
            ./emqx/bin/emqx_ctl status
            ./emqx/bin/emqx versions
            cp "${PACKAGE_PATH}/${EMQX_NAME}"-*-"${EMQX_DEPS_DEFAULT_VSN#v}-$(uname -m)".zip ./emqx/releases
            ./emqx/bin/emqx install "${EMQX_DEPS_DEFAULT_VSN#v}"
            [ "$(./emqx/bin/emqx versions |grep permanent | grep -oE "[0-9].[0-9].[0-9]")" = "${EMQX_DEPS_DEFAULT_VSN#v}" ] || exit 1
            ./emqx/bin/emqx_ctl status
            ./emqx/bin/emqx stop
            rm -rf emqx
        done
   fi
}

emqx_prepare
emqx_test
relup_test
