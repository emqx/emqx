#!/usr/bin/env bash

set -euo pipefail

# Test script for find-apps.sh --ci --base-ref
# This script:
# 1. Remembers the current HEAD commit
# 2. Makes changes in a few apps/appname directories
# 3. Asserts find-apps.sh finds the changed apps and their user apps
# 4. Resets git to the original commit

SCRIPT_DIR="$(cd -P -- "$(dirname -- "$0")" && pwd)"
PROJECT_ROOT="$(cd -P -- "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Save original HEAD commit
ORIGINAL_HEAD=$(git rev-parse HEAD)
echo -e "${YELLOW}Original HEAD: ${ORIGINAL_HEAD}${NC}"

# Cleanup function to reset git and remove test files
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    # Remove temporary test files created during tests
    find . -type f -name "test-change.*.tmp" -delete 2>/dev/null || true
    git reset --hard "${ORIGINAL_HEAD}" >/dev/null 2>&1 || true
    echo -e "${GREEN}Reset to original commit${NC}"
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Test function
test_find_apps() {
    local test_name="$1"
    local expected_apps="$2"
    local base_ref="$3"

    echo -e "\n${YELLOW}Test: ${test_name}${NC}"
    echo "Expected apps: ${expected_apps}"

    # Run find-apps.sh
    local output
    output=$(./scripts/find-apps.sh --ci --base-ref "${base_ref}" 2>&1)
    local exit_code=$?

    if [ ${exit_code} -ne 0 ]; then
        echo -e "${RED}FAIL: find-apps.sh exited with code ${exit_code}${NC}"
        echo "Output: ${output}"
        return 1
    fi

    # Parse JSON output to extract app paths
    local found_apps
    found_apps=$(echo "${output}" | jq -r '.[].app' | sort | tr '\n' ' ' | xargs)

    echo "Found apps: ${found_apps}"

    # Check if all expected apps are present
    local missing_apps=""
    for expected_app in ${expected_apps}; do
        if ! echo "${found_apps}" | grep -q "\b${expected_app}\b"; then
            missing_apps="${missing_apps} ${expected_app}"
        fi
    done

    if [ -n "${missing_apps}" ]; then
        echo -e "${RED}FAIL: Missing expected apps:${missing_apps}${NC}"
        return 1
    fi

    echo -e "${GREEN}PASS${NC}"
    return 0
}

# Make a dummy change in an app directory and commit it
make_change() {
    local app_path="$1"
    local test_file
    test_file="${app_path}/test-change.$(date +%s).tmp"

    mkdir -p "${app_path}"
    echo "# Test change for dependency testing" > "${test_file}"
    git add "${test_file}" >/dev/null 2>&1 || true
    git commit -m "test: change ${app_path} for dependency testing" >/dev/null 2>&1 || true
}

# Test 1: Change an app that is used by other apps
echo -e "\n${YELLOW}=== Test 1: Change app used by others ===${NC}"

# Find an app that has users (not "all" or "none")
# Let's use emqx_auto_subscribe which is used by emqx_telemetry
TEST_APP="emqx_auto_subscribe"
TEST_APP_PATH="apps/${TEST_APP}"

if [ ! -d "${TEST_APP_PATH}" ]; then
    echo -e "${RED}SKIP: ${TEST_APP_PATH} does not exist${NC}"
else
    make_change "${TEST_APP_PATH}"

    # According to deps.txt, emqx_auto_subscribe is used by emqx_telemetry
    # So we expect both apps to be in the output
    test_find_apps \
        "Change ${TEST_APP}" \
        "apps/${TEST_APP} apps/emqx_telemetry" \
        "${ORIGINAL_HEAD}"

    # Reset for next test
    find . -type f -name "test-change.*.tmp" -delete 2>/dev/null || true
    git reset --hard "${ORIGINAL_HEAD}" >/dev/null 2>&1
fi

# Test 2: Change an app that is used by "all"
echo -e "\n${YELLOW}=== Test 2: Change app used by all ===${NC}"

TEST_APP="emqx_bridge"
TEST_APP_PATH="apps/${TEST_APP}"

if [ ! -d "${TEST_APP_PATH}" ]; then
    echo -e "${RED}SKIP: ${TEST_APP_PATH} does not exist${NC}"
else
    make_change "${TEST_APP_PATH}"

    # According to deps.txt, emqx_bridge is used by "all"
    # So we expect all apps to be in the output
    all_apps_output=$(./scripts/find-apps.sh --ci --base-ref "${ORIGINAL_HEAD}" 2>&1)
    all_apps_count=$(echo "${all_apps_output}" | jq 'length')

    if [ "${all_apps_count}" -gt 10 ]; then
        echo -e "${GREEN}PASS: Found ${all_apps_count} apps (expected many/all)${NC}"
    else
        echo -e "${RED}FAIL: Only found ${all_apps_count} apps, expected many/all${NC}"
        exit 1
    fi

    # Reset for next test
    find . -type f -name "test-change.*.tmp" -delete 2>/dev/null || true
    git reset --hard "${ORIGINAL_HEAD}" >/dev/null 2>&1
fi

# Test 3: Change an app with "none" users
echo -e "\n${YELLOW}=== Test 3: Change app used by none ===${NC}"

TEST_APP="emqx_auth_cinfo"
TEST_APP_PATH="apps/${TEST_APP}"

if [ ! -d "${TEST_APP_PATH}" ]; then
    echo -e "${RED}SKIP: ${TEST_APP_PATH} does not exist${NC}"
else
    make_change "${TEST_APP_PATH}"

    # According to deps.txt, emqx_auth_cinfo is used by "none"
    # So we expect only the changed app itself
    test_find_apps \
        "Change ${TEST_APP}" \
        "apps/${TEST_APP}" \
        "${ORIGINAL_HEAD}"

    # Reset for next test
    find . -type f -name "test-change.*.tmp" -delete 2>/dev/null || true
    git reset --hard "${ORIGINAL_HEAD}" >/dev/null 2>&1
fi

# Test 4: Change multiple apps
echo -e "\n${YELLOW}=== Test 4: Change multiple apps ===${NC}"

TEST_APP1="emqx_auth_cinfo"
TEST_APP2="emqx_auth_ext"
TEST_APP_PATH1="apps/${TEST_APP1}"
TEST_APP_PATH2="apps/${TEST_APP2}"

if [ ! -d "${TEST_APP_PATH1}" ] || [ ! -d "${TEST_APP_PATH2}" ]; then
    echo -e "${RED}SKIP: One or both test apps do not exist${NC}"
else
    make_change "${TEST_APP_PATH1}"
    make_change "${TEST_APP_PATH2}"

    # Both apps are used by "none", so we expect both apps
    test_find_apps \
        "Change ${TEST_APP1} and ${TEST_APP2}" \
        "apps/${TEST_APP1} apps/${TEST_APP2}" \
        "${ORIGINAL_HEAD}"

    # Reset for next test
    find . -type f -name "test_change_*.tmp" -delete 2>/dev/null || true
    git reset --hard "${ORIGINAL_HEAD}" >/dev/null 2>&1
fi

# Test 5: Change rebar.config (should trigger all apps)
echo -e "\n${YELLOW}=== Test 5: Change rebar.config ===${NC}"

if [ ! -f "rebar.config" ]; then
    echo -e "${RED}SKIP: rebar.config does not exist${NC}"
else
    echo "# Test change" >> rebar.config
    git add rebar.config >/dev/null 2>&1
    git commit -m "test: change rebar.config" >/dev/null 2>&1 || true

    all_apps_output=$(./scripts/find-apps.sh --ci --base-ref "${ORIGINAL_HEAD}" 2>&1)
    all_apps_count=$(echo "${all_apps_output}" | jq 'length')

    if [ "${all_apps_count}" -gt 10 ]; then
        echo -e "${GREEN}PASS: Found ${all_apps_count} apps (expected all)${NC}"
    else
        echo -e "${RED}FAIL: Only found ${all_apps_count} apps, expected all${NC}"
        exit 1
    fi

    # Reset for next test
    find . -type f -name "test-change.*.tmp" -delete 2>/dev/null || true
    git reset --hard "${ORIGINAL_HEAD}" >/dev/null 2>&1
fi

# Test 6: Change .github directory (should trigger all apps)
echo -e "\n${YELLOW}=== Test 6: Change .github directory ===${NC}"

if [ ! -d ".github" ]; then
    echo -e "${RED}SKIP: .github directory does not exist${NC}"
else
    mkdir -p .github
    echo "# Test change" > ".github/test-change.$(date +%s).tmp"
    git add .github/test-change.*.tmp >/dev/null 2>&1
    git commit -m "test: change .github directory" >/dev/null 2>&1 || true

    all_apps_output=$(./scripts/find-apps.sh --ci --base-ref "${ORIGINAL_HEAD}" 2>&1)
    all_apps_count=$(echo "${all_apps_output}" | jq 'length')

    if [ "${all_apps_count}" -gt 10 ]; then
        echo -e "${GREEN}PASS: Found ${all_apps_count} apps (expected all)${NC}"
    else
        echo -e "${RED}FAIL: Only found ${all_apps_count} apps, expected all${NC}"
        exit 1
    fi

    # Reset for next test
    find . -type f -name "test-change.*.tmp" -delete 2>/dev/null || true
    git reset --hard "${ORIGINAL_HEAD}" >/dev/null 2>&1
fi

echo -e "\n${GREEN}All tests passed!${NC}"
