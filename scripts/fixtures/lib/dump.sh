# shellcheck shell=bash
#
# Fixture-JSON synthesis. Pulls the requested state sections from
# java-tron's HTTP API (via lib/api.sh) and writes a fixture.json
# conforming to schema v1 (see
# docs/superpowers/specs/2026-04-15-fixture-extraction-design.md §3.3).
#
# Usage:
#   dump_fixture <output_path> <scenario_name> <config_path> <http_port> \
#                --section dp \
#                [--section accounts <base58>[,<base58>...]] \
#                [--section receipts <txid>[,<txid>...]]

: "${FULLNODE_JAR:=/Users/asuka/Projects/tron/java-tron/build/libs/FullNode.jar}"
: "${JAVA:=java}"

_dump_java_tron_version() {
    # "--version" is not a standard java-tron arg; we best-effort grep the
    # manifest. Fall back to "unknown" rather than failing — the sha256 of
    # the jar pins the observed binary well enough for PR review.
    if command -v unzip >/dev/null 2>&1; then
        local ver
        ver=$(unzip -p "$FULLNODE_JAR" META-INF/MANIFEST.MF 2>/dev/null \
            | tr -d '\r' \
            | awk -F': ' '/^Implementation-Version:/ {print $2; exit}')
        if [[ -n "$ver" ]]; then
            printf '%s' "$ver"
            return 0
        fi
    fi
    printf 'unknown'
}

_dump_jar_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$FULLNODE_JAR" | awk '{print $1}'
    else
        shasum -a 256 "$FULLNODE_JAR" | awk '{print $1}'
    fi
}

_dump_config_sha256() {
    local config="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$config" | awk '{print $1}'
    else
        shasum -a 256 "$config" | awk '{print $1}'
    fi
}

# _dump_dp <port> → JSON object like {"KEY": value, ...} on stdout.
# Fails if /wallet/getchainparameters returns an empty list.
_dump_dp() {
    local port="$1"
    local raw
    raw=$(api_get_chain_parameters "$port") || return 1

    local normalised
    normalised=$(printf '%s' "$raw" | jq -c '
        .chainParameter // [] |
        map({key: .key, value: (.value // 0)}) |
        from_entries
    ') || {
        echo "dump_dp: jq normalisation failed" >&2
        return 1
    }

    local count
    count=$(printf '%s' "$normalised" | jq 'length')
    if (( count == 0 )); then
        echo "dump_dp: getchainparameters returned zero entries" >&2
        return 1
    fi

    printf '%s' "$normalised"
}

# _dump_block_meta <port> → JSON object {"blockNum":N, "blockHash":"0x..."}
_dump_block_meta() {
    local port="$1"
    local raw
    raw=$(api_get_now_block "$port") || return 1
    printf '%s' "$raw" | jq -c '{
        blockNum: (.block_header.raw_data.number // 0),
        blockHash: ("0x" + (.blockID // ""))
    }'
}

# dump_fixture <output_path> <scenario_name> <config_path> <http_port> --section dp ...
dump_fixture() {
    local output_path="$1"
    local scenario="$2"
    local config="$3"
    local port="$4"
    shift 4

    if [[ -z "$output_path" || -z "$scenario" || -z "$config" || -z "$port" ]]; then
        echo "dump_fixture: usage: dump_fixture <output> <scenario> <config> <port> --section ..." >&2
        return 1
    fi
    if ! command -v jq >/dev/null 2>&1; then
        echo "dump_fixture: jq is required but not installed" >&2
        return 1
    fi

    local want_dp=0
    local accounts_csv=""
    local receipts_csv=""

    while (( $# > 0 )); do
        case "$1" in
            --section)
                shift
                case "${1:-}" in
                    dp) want_dp=1; shift ;;
                    accounts) accounts_csv="${2:-}"; shift 2 ;;
                    receipts) receipts_csv="${2:-}"; shift 2 ;;
                    *) echo "dump_fixture: unknown section '${1:-}'" >&2; return 1 ;;
                esac
                ;;
            *) echo "dump_fixture: unknown argument '$1'" >&2; return 1 ;;
        esac
    done

    local dp_json="null"
    if (( want_dp )); then
        dp_json=$(_dump_dp "$port") || return 1
    fi

    # TODO(M1.2): when a scenario first needs account / receipt dumps,
    # implement these in place of the null placeholders. The schema already
    # accommodates both as optional maps.
    local accounts_json="null"
    if [[ -n "$accounts_csv" ]]; then
        echo "dump_fixture: --section accounts not yet implemented (TODO M1.2)" >&2
        return 1
    fi
    local receipts_json="null"
    if [[ -n "$receipts_csv" ]]; then
        echo "dump_fixture: --section receipts not yet implemented (TODO M1.2)" >&2
        return 1
    fi

    local block_meta
    block_meta=$(_dump_block_meta "$port") || return 1

    local jt_version jar_sha config_sha extracted_at
    jt_version=$(_dump_java_tron_version)
    jar_sha=$(_dump_jar_sha256)
    config_sha=$(_dump_config_sha256 "$config")
    extracted_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    mkdir -p "$(dirname "$output_path")"

    jq -n \
        --argjson schema 1 \
        --arg scenario "$scenario" \
        --arg version "$jt_version" \
        --arg jarSha "$jar_sha" \
        --arg configSha "$config_sha" \
        --arg extractedAt "$extracted_at" \
        --argjson blockMeta "$block_meta" \
        --argjson dp "$dp_json" \
        --argjson accounts "$accounts_json" \
        --argjson receipts "$receipts_json" \
        '{
            schema: $schema,
            scenario: $scenario,
            javaTron: {
                version: $version,
                jarSha256: $jarSha,
                configSha256: $configSha
            },
            extractedAt: $extractedAt,
            blockNum: $blockMeta.blockNum,
            blockHash: $blockMeta.blockHash,
            dynamicProperties: $dp,
            accounts: $accounts,
            receipts: $receipts
        }
        | with_entries(select(.value != null))' > "$output_path"

    if ! jq empty < "$output_path"; then
        echo "dump_fixture: produced invalid JSON at $output_path" >&2
        return 1
    fi
    return 0
}
