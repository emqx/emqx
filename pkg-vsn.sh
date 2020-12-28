#!/bin/bash

# This script prints the release version for emqx

# ensure dir
cd -P -- "$(dirname -- "$0")"

# comment SUFFIX out when finalising RELEASE
RELEASE="4.3.0"
SUFFIX="-pre-$(git rev-parse HEAD | cut -b1-8)"

echo "${RELEASE}${SUFFIX:-}"
