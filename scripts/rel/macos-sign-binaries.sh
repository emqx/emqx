#!/usr/bin/env bash

# intended to run on MacOS only
# signs executables and runtime libraries found in $RELX_TEMP_DIR with developer certificate

# required variables:
# APPLE_DEVELOPER_IDENTITY: "Developer ID Application: <company name> (<hex id>)"
# APPLE_DEVELOPER_ID_BUNDLE: base64-encoded content of apple developer id certificate bundle in pksc12 format
# APPLE_DEVELOPER_ID_BUNDLE_PASSWORD: password used when exporting the bundle

# note: 'bundle' in apple terminology is 'identity'

set -euo pipefail

if [ "$(uname)" != 'Darwin' ]; then
    echo 'Not macOS, exiting';
    exit 0;
fi

if [ "${APPLE_SIGN_BINARIES:-0}" == 0 ]; then
    echo "Signing Apple binaries is disabled, exiting"
    exit 0
fi

if [[ "${APPLE_DEVELOPER_ID_BUNDLE:-0}" == 0 || "${APPLE_DEVELOPER_ID_BUNDLE_PASSWORD:-0}" == 0 ]]; then
    echo "Apple developer certificate is not configured, skip signing"
    exit 0
fi

if [ -n "${RELX_TEMP_DIR:-}" ]; then
  pushd "${RELX_TEMP_DIR}"
fi

PKSC12_FILE="$HOME/developer-id-application.p12"
printf '%s' "${APPLE_DEVELOPER_ID_BUNDLE}" | base64 --decode > "${PKSC12_FILE}"

KEYCHAIN="emqx-$(date +%s).keychain-db"
KEYCHAIN_PASSWORD="$(openssl rand -base64 32)"

trap cleanup EXIT

function cleanup {
    set +e
    security delete-keychain "${KEYCHAIN}" 2>/dev/null
    if [ -n "${RELX_TEMP_DIR:-}" ]; then
      cd "${RELX_TEMP_DIR}"
    fi
    rm -f certificate.crt
}

security create-keychain -p "${KEYCHAIN_PASSWORD}" "${KEYCHAIN}"
security set-keychain-settings "${KEYCHAIN}"
security unlock-keychain -p "${KEYCHAIN_PASSWORD}" "${KEYCHAIN}"
security import "${PKSC12_FILE}" -P "${APPLE_DEVELOPER_ID_BUNDLE_PASSWORD}" -t cert -f pkcs12 -k "${KEYCHAIN}" -T /usr/bin/codesign
security set-key-partition-list -S "apple-tool:,apple:,codesign:" -s -k "${KEYCHAIN_PASSWORD}" "${KEYCHAIN}"
if [ "$(sw_vers -productVersion | cut -d'.' -f1)" -ge 15 ]; then
  openssl pkcs12 -in "${PKSC12_FILE}" -clcerts -nokeys -out certificate.crt --passin "pass:${APPLE_DEVELOPER_ID_BUNDLE_PASSWORD}"
  security verify-cert -k "${KEYCHAIN}" -c certificate.crt
else
  security verify-cert -k "${KEYCHAIN}" -c "${PKSC12_FILE}"
fi
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

find . \( \
    -name asn1rt_nif.so \
    -o -name bcrypt_nif.so \
    -o -name beam.smp \
    -o -name cpu_sup \
    -o -name crc32cer_nif.so \
    -o -name crypto.so \
    -o -name crypto_callback.so \
    -o -name dyn_erl \
    -o -name dyntrace.so \
    -o -name epmd \
    -o -name erl \
    -o -name erl_call \
    -o -name erl_child_setup \
    -o -name erlang_jq_port \
    -o -name erlexec \
    -o -name escript \
    -o -name ezstd_nif.so \
    -o -name heart \
    -o -name inet_gethost \
    -o -name jiffy.so \
    -o -name jq_nif1.so \
    -o -name liberocksdb.so \
    -o -name libjq.1.dylib \
    -o -name 'libmsquic*.dylib' \
    -o -name libonig.5.dylib \
    -o -name 'libquicer_nif*.dylib' \
    -o -name libquicer_nif.so \
    -o -name memsup \
    -o -name odbcserver \
    -o -name otp_test_engine.so \
    -o -name run_erl \
    -o -name sasl_auth.so \
    -o -name snappyer.so \
    -o -name to_erl \
    -o -name trace_file_drv.so \
    -o -name trace_ip_drv.so \
\) -print0 | xargs -0 --no-run-if-empty codesign -s "${APPLE_DEVELOPER_IDENTITY}" -f --verbose=4 --timestamp --options=runtime

if [ -n "${RELX_TEMP_DIR:-}" ]; then
  popd
fi

cleanup
