#!/usr/bin/env bash

# intended to run on MacOS only
# signs all executable files in a given folder (as $1) with developer certificate

# required variables:
# APPLE_DEVELOPER_IDENTITY: "Developer ID Application: <company name> (<hex id>)"
# APPLE_DEVELOPER_ID_BUNDLE: base64-encoded content of apple developer id certificate bundle in pksc12 format
# APPLE_DEVELOPER_ID_BUNDLE_PASSWORD: password used when exporting the bundle

# note: 'bundle' in apple terminology is 'identity'

set -euo pipefail

if [[ "${APPLE_DEVELOPER_ID_BUNDLE:-0}" == 0 || "${APPLE_DEVELOPER_ID_BUNDLE_PASSWORD:-0}" == 0 ]]; then
    echo "Apple developer certificate is not configured, skip signing"
    exit 0
fi

REL_DIR="${1}"
PKSC12_FILE="$HOME/developer-id-application.p12"
base64 --decode > "${PKSC12_FILE}" <<<"${APPLE_DEVELOPER_ID_BUNDLE}"

KEYCHAIN="emqx-$(date +%s).keychain-db"
KEYCHAIN_PASSWORD="$(openssl rand -base64 32)"

trap cleanup EXIT

function cleanup {
    set +e
    security delete-keychain "${KEYCHAIN}" 2>/dev/null
}

security create-keychain -p "${KEYCHAIN_PASSWORD}" "${KEYCHAIN}"
security set-keychain-settings -lut 21600 "${KEYCHAIN}"
security unlock-keychain -p "${KEYCHAIN_PASSWORD}" "${KEYCHAIN}"
security import "${PKSC12_FILE}" -P "${APPLE_DEVELOPER_ID_BUNDLE_PASSWORD}" -t cert -f pkcs12 -k "${KEYCHAIN}" -T /usr/bin/codesign
security set-key-partition-list -S "apple-tool:,apple:,codesign:" -s -k "${KEYCHAIN_PASSWORD}" "${KEYCHAIN}"
security verify-cert -k "${KEYCHAIN}" -c "${PKSC12_FILE}"
security find-identity -p codesigning "${KEYCHAIN}"

# add new keychain into the search path for codesign, otherwise the stuff does not work
keychains=$(security list-keychains -d user)
keychain_names=();
for keychain in ${keychains}; do
    basename=$(basename "${keychain}")
    keychain_name=${basename::${#basename}-4}
    keychain_names+=("${keychain_name}")
done
security -v list-keychains -s "${keychain_names[@]}" "${KEYCHAIN}"

# known runtime executables and binaries
codesign -s "${APPLE_DEVELOPER_IDENTITY}" -f --verbose=4 --timestamp --options=runtime \
         "${REL_DIR}"/erts-*/bin/{beam.smp,dyn_erl,epmd,erl,erl_call,erl_child_setup,erlexec,escript,heart,inet_gethost,run_erl,to_erl}
codesign -s "${APPLE_DEVELOPER_IDENTITY}" -f --verbose=4 --timestamp --options=runtime \
         "${REL_DIR}"/lib/runtime_tools-*/priv/lib/{dyntrace.so,trace_ip_drv.so,trace_file_drv.so}
codesign -s "${APPLE_DEVELOPER_IDENTITY}" -f --verbose=4 --timestamp --options=runtime \
         "${REL_DIR}"/lib/os_mon-*/priv/bin/{cpu_sup,memsup}
# other files from runtime and dependencies
for f in \
        asn1rt_nif.so \
        bcrypt_nif.so \
        crypto.so \
        otp_test_engine.so \
        crypto_callback.so \
        jiffy.so \
        crc32cer_nif.so \
        sasl_auth.so \
        snappyer.so \
        odbcserver \
        ezstd_nif.so \
    ; do
    find "${REL_DIR}"/lib/ -name "$f" -exec codesign -s "${APPLE_DEVELOPER_IDENTITY}" -f --verbose=4 --timestamp --options=runtime {} \;
done

cleanup
