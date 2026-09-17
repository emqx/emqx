#!/usr/bin/env bash

# Export installed runtime packages from a DHI dev image. Copying package file
# lists (including SASL plugins and their dependencies) avoids pulling the dev
# image's package manager and unrelated libraries into the runtime image.
set -euo pipefail

rootfs="$1"
shift

workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT

# Include Depends and Pre-Depends, but no optional or conflicting packages.
apt-cache depends --recurse --installed \
    --no-recommends --no-suggests --no-conflicts --no-breaks \
    --no-replaces --no-enhances "$@" \
    | awk '/^[a-z0-9][a-z0-9+.:_-]*$/ { print }' \
    | sort -u > "$workdir/packages"

mkdir -p "$rootfs/var/lib/dpkg/info" "$rootfs/var/lib/dpkg/status.d"
cp "$rootfs/var/lib/dpkg/status" "$workdir/base-status"
: > "$workdir/added-status"

while IFS= read -r package; do
    case "$package" in
        apt|apt:*|dpkg|dpkg:*|libapt-*|libgnutls*)
            echo "Unexpected runtime dependency: $package" >&2
            exit 1
            ;;
    esac

    binary_package=$(dpkg-query -W -f='${binary:Package}' "$package")
    file_list="$rootfs/var/lib/dpkg/info/$binary_package.list"
    dpkg-query -L "$package" > "$file_list"
    while IFS= read -r path; do
        # Never copy whole directories such as /usr. Some files listed in the
        # package database have already been removed by DHI's image build.
        if [[ -f "$path" || -L "$path" ]]; then
            cp -a --parents "$path" "$rootfs"
        fi
    done < "$file_list"

    # Keep both Debian and DHI package records current for SBOM/CVE scanners.
    dpkg-query -s "$package" > "$rootfs/var/lib/dpkg/status.d/${package%%:*}"
    cat "$rootfs/var/lib/dpkg/status.d/${package%%:*}" >> "$workdir/added-status"
    printf '\n' >> "$workdir/added-status"
done < "$workdir/packages"

# Merge package records, replacing the base record when a dependency was also
# present there. Do not copy the dev image's complete package database.
awk '
    BEGIN { RS = ""; FS = "\n"; ORS = "\n\n" }
    {
        package = ""; architecture = ""
        for (i = 1; i <= NF; i++) {
            if ($i ~ /^Package: /) package = $i
            if ($i ~ /^Architecture: /) architecture = $i
        }
        records[package SUBSEP architecture] = $0
    }
    END { for (key in records) print records[key] }
' "$workdir/base-status" "$workdir/added-status" > "$rootfs/var/lib/dpkg/status"

# update-alternatives creates these links after installation, so they are not
# part of mawk's file list. Use a direct link without shipping dpkg's tooling.
ln -s mawk "$rootfs/usr/bin/awk"
