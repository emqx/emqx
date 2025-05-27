#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -euo pipefail

# Function for logging to stderr
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" >&2
}

# --- Input Validation & Setup ---
GH_REPO=${1:-}
ISSUE_NUMBER=${2:-}
GEMINI_API_KEY="${GEMINI_API_KEY:-}"
GEMINI_MODEL="${GEMINI_MODEL:-gemini-2.5-flash-preview-05-20}"
GH_TOKEN="${GH_TOKEN:-}"
GITHUB_ACTIONS="${GITHUB_ACTIONS:-false}"

if [ -z "$GH_REPO" ]; then log "Error: Repository missing."; exit 1; fi
if [ -z "$ISSUE_NUMBER" ]; then log "Error: Issue number missing."; exit 1; fi
if [ -z "$GEMINI_API_KEY" ]; then log "Error: GEMINI_API_KEY missing."; exit 1; fi
if ! command -v gh &> /dev/null; then log "Error: 'gh' not found."; exit 1; fi
if ! command -v jq &> /dev/null; then log "Error: 'jq' not found."; exit 1; fi
if ! command -v curl &> /dev/null; then log "Error: 'curl' not found."; exit 1; fi
if ! command -v grep &> /dev/null; then log "Error: 'grep' not found."; exit 1; fi
if ! command -v file &> /dev/null; then log "Error: 'file' not found."; exit 1; fi
if ! command -v base64 &> /dev/null; then log "Error: 'base64' not found."; exit 1; fi
if [ -z "${GH_TOKEN}" ] && [ "${GITHUB_ACTIONS}" = "true" ]; then log "Error: GH_TOKEN missing in Actions."; exit 1; fi

API_URL="https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}"
TMP_DIR=$(mktemp -d) # Create a temporary directory for images

# --- Cleanup Trap ---
# Ensure temporary directory is removed on exit
trap 'log "Cleaning up temporary directory $TMP_DIR..."; rm -rf "$TMP_DIR"' EXIT

# --- Fetch Issue Details ---
log "Fetching issue $ISSUE_NUMBER from $GH_REPO..."
ISSUE_DATA=$(GH_TOKEN=${GH_TOKEN} gh issue view "$ISSUE_NUMBER" --json title,body --repo "$GH_REPO")
ISSUE_TITLE=$(echo "$ISSUE_DATA" | jq -r '.title // ""')
ISSUE_BODY=$(echo "$ISSUE_DATA" | jq -r '.body // ""')

if [ -z "$ISSUE_TITLE" ] && [ -z "$ISSUE_BODY" ]; then
    log "Error: Could not fetch title or body for issue $ISSUE_NUMBER."
    exit 1
fi

log "Issue Title: $ISSUE_TITLE"

# --- Function to Call Gemini API ---
call_gemini() {
    local payload="$1"
    local response
    response=$(curl -s --fail -H 'Content-Type: application/json' -X POST "$API_URL" -d "$payload")
    # shellcheck disable=SC2181
    if [ $? -ne 0 ]; then
        log "Error: curl command failed."
        log "Response: ${response:-No response}"
        echo "Error: API call failed"
        return 1
    fi
    if echo "$response" | jq -e '.error' > /dev/null; then
        # shellcheck disable=SC2155
        local error_msg=$(echo "$response" | jq -r '.error.message // "Unknown API error"')
        log "Error: API call failed - $error_msg"
        echo "Error: API call failed - $error_msg"
        return 1
    fi
    echo "$response" | jq -r '.candidates[0].content.parts[0].text // ""'
}

# --- Translate Text ---
log "Translating issue text..."
TEXT_PROMPT="You are a translation service for GitHub issues. Your task is to translate the following title and body to English *only if* their primary language is Chinese.
1. If the input is *not* primarily Chinese, you MUST respond *only* with the exact text: NO_TRANSLATION_NEEDED
2. If the input *is* primarily Chinese, translate both the title and the body to English.
3. Your response MUST be formatted as follows:
   - The *first line* must contain *only* the translated title.
   - All *subsequent lines* must contain *only* the translated body.
   - Do *not* add any prefixes or other text.
Input:
Title: $ISSUE_TITLE
Body: $ISSUE_BODY"

TEXT_PAYLOAD=$(jq -n --arg prompt "$TEXT_PROMPT" '{ "contents":[ { "parts":[ { "text": $prompt } ] } ] }')
TEXT_TRANSLATION=$(call_gemini "$TEXT_PAYLOAD")

if [[ "$TEXT_TRANSLATION" == Error:* ]]; then
    log "Text translation failed."
    echo "$TEXT_TRANSLATION"
    exit 1
fi

# --- Find & Translate Images ---
log "Searching for images in the issue body..."
IMAGE_URL_PATTERN="https://github.com/${GH_REPO}/assets/[0-9]+/[0-9a-fA-F-]+"
IMAGE_URLS=$(echo "$ISSUE_BODY" | grep -oE "!\[[^]]*\]\((${IMAGE_URL_PATTERN})\)" | sed -E 's/!\[[^]]*\]\((.*)\)/\1/' || true)

SCREENSHOT_TRANSLATIONS=""
IMG_COUNT=0

if [ -n "$IMAGE_URLS" ]; then
    log "Found image URLs. Processing..."
    while IFS= read -r URL; do
        ((IMG_COUNT++))
        IMG_FILENAME="$TMP_DIR/image_$IMG_COUNT"
        log "Processing Image $IMG_COUNT: $URL"

        # Download image
        if ! curl -sL --fail -o "$IMG_FILENAME" "$URL"; then
            log "Warning: Failed to download image $IMG_COUNT ($URL). Skipping."
            continue
        fi

        # Get MIME type
        MIME_TYPE=$(file --mime-type -b "$IMG_FILENAME")
        if [[ ! "$MIME_TYPE" == image/* ]]; then
            log "Warning: Downloaded file for image $IMG_COUNT ($URL) is not an image ($MIME_TYPE). Skipping."
            continue
        fi

        # Base64 encode
        B64_DATA=$(base64 < "$IMG_FILENAME" | tr -d '\n')

        # Prepare image prompt & payload
        IMG_PROMPT="Translate any Chinese text in this image to English. If no Chinese text is found, respond *only* with 'NO_CHINESE_TEXT_FOUND'."
        IMG_PAYLOAD=$(jq -n --arg prompt "$IMG_PROMPT" --arg mime "$MIME_TYPE" --arg data "$B64_DATA" \
            '{ "contents":[ { "parts":[ { "text": $prompt }, { "inline_data": { "mime_type": $mime, "data": $data } } ] } ] }')

        # Call Gemini and process response
        IMG_TRANS=$(call_gemini "$IMG_PAYLOAD")

        if [[ "$IMG_TRANS" != "NO_CHINESE_TEXT_FOUND" && "$IMG_TRANS" != Error:* && -n "$IMG_TRANS" ]]; then
            log "Image $IMG_COUNT: Translation found."
            SCREENSHOT_TRANSLATIONS+="\n\n**Screenshot $IMG_COUNT Translation:**\n${IMG_TRANS}"
        else
            log "Image $IMG_COUNT: No Chinese text found or error."
        fi

    done <<< "$IMAGE_URLS"
fi

# --- Combine & Output ---
# Check if any translation happened (text or image)
if [[ "$TEXT_TRANSLATION" == "NO_TRANSLATION_NEEDED" && -z "$SCREENSHOT_TRANSLATIONS" ]]; then
    log "No translation needed for text or images."
    echo "NO_TRANSLATION_NEEDED"
else
    # If text wasn't translated but images were, use original text
    if [[ "$TEXT_TRANSLATION" == "NO_TRANSLATION_NEEDED" ]]; then
        FINAL_OUTPUT="$ISSUE_TITLE\n$ISSUE_BODY"
    else
        FINAL_OUTPUT="$TEXT_TRANSLATION"
    fi

    # Append image translations if they exist
    if [ -n "$SCREENSHOT_TRANSLATIONS" ]; then
        FINAL_OUTPUT+="\n\n---\n## Translated Screenshots$SCREENSHOT_TRANSLATIONS"
    fi

    echo -e "$FINAL_OUTPUT"
fi

log "Script finished."
