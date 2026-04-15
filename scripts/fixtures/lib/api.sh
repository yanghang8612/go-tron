# shellcheck shell=bash
#
# Thin curl wrappers over java-tron's /wallet/* HTTP API. Each function
# prints the raw JSON response on stdout and returns non-zero on
# transport error or when the response body contains an "Error" key.
# Parsing/normalisation happens in lib/dump.sh, not here.

_api_call() {
    local port="$1"
    local path="$2"
    local method="${3:-GET}"
    local body="${4:-}"

    local url="http://127.0.0.1:${port}${path}"
    local curl_args=(-sS -m 5 -o - -w '\n%{http_code}')

    local response
    if [[ "$method" == "POST" ]]; then
        response=$(curl "${curl_args[@]}" -X POST \
            -H "Content-Type: application/json" \
            --data "$body" "$url") || return 1
    else
        response=$(curl "${curl_args[@]}" "$url") || return 1
    fi

    local status="${response##*$'\n'}"
    local payload="${response%$'\n'*}"

    if [[ "$status" -lt 200 || "$status" -ge 300 ]]; then
        echo "api: $method $url -> HTTP $status" >&2
        echo "$payload" >&2
        return 1
    fi

    # java-tron returns 200 with an "Error" field on logical failures.
    if printf '%s' "$payload" | grep -q '"Error"'; then
        echo "api: $method $url returned logical error:" >&2
        echo "$payload" >&2
        return 1
    fi

    printf '%s' "$payload"
    return 0
}

api_get_now_block() {
    local port="$1"
    _api_call "$port" "/wallet/getnowblock" GET
}

api_get_block_by_num() {
    local port="$1"
    local num="$2"
    _api_call "$port" "/wallet/getblockbynum" POST "{\"num\":${num}}"
}

api_get_chain_parameters() {
    local port="$1"
    _api_call "$port" "/wallet/getchainparameters" GET
}

api_get_account() {
    local port="$1"
    local base58="$2"
    _api_call "$port" "/wallet/getaccount" POST \
        "{\"address\":\"${base58}\",\"visible\":true}"
}
