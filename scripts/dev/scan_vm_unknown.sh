#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <start-block-inclusive> <end-block-exclusive>" >&2
  exit 2
fi

start_block=$1
end_block=$2
api_base=${TRON_HTTP_API:-https://api.trongrid.io}
chunk_size=${TRON_SCAN_CHUNK:-10}

if [[ ! $start_block =~ ^[0-9]+$ || ! $end_block =~ ^[0-9]+$ ]] ||
  (( start_block < 0 || end_block <= start_block )); then
  echo "invalid block range: [$start_block, $end_block)" >&2
  exit 2
fi
if [[ ! $chunk_size =~ ^[0-9]+$ ]] || (( chunk_size < 1 || chunk_size > 100 )); then
  echo "TRON_SCAN_CHUNK must be between 1 and 100" >&2
  exit 2
fi

post_json() {
  local endpoint=$1
  local payload=$2
  local response
  local attempt

  for attempt in 1 2 3 4 5; do
    if response=$(curl --fail --silent --show-error --http1.1 \
      --connect-timeout 10 --max-time 120 \
      -H 'Content-Type: application/json' \
      -X POST "$api_base/$endpoint" \
      --data "$payload") && jq -e . >/dev/null 2>&1 <<<"$response"; then
      printf '%s' "$response"
      return 0
    fi
    echo "request failed ($attempt/5): $endpoint" >&2
    sleep 1
  done
  return 1
}

printf 'block\ttx_id\tcontract_ret\tmessage\n'

cursor=$start_block
while (( cursor < end_block )); do
  next=$((cursor + chunk_size))
  if (( next > end_block )); then
    next=$end_block
  fi

  blocks=$(post_json wallet/getblockbylimitnext \
    "{\"startNum\":$cursor,\"endNum\":$next}")

  while IFS=$'\t' read -r block_number tx_id contract_ret; do
    [[ -n "$tx_id" ]] || continue
    info=$(post_json wallet/gettransactioninfobyid \
      "{\"value\":\"$tx_id\"}")
    message_hex=$(jq -r '.resMessage // ""' <<<"$info")
    message=$(printf '%s' "$message_hex" | xxd -r -p 2>/dev/null || true)
    message=${message//$'\t'/ }
    message=${message//$'\r'/ }
    message=${message//$'\n'/\\n}
    printf '%s\t%s\t%s\t%s\n' "$block_number" "$tx_id" "$contract_ret" "$message"
  done < <(jq -r '
    .block[]?
    | .block_header.raw_data.number as $number
    | .transactions[]?
    | select(.ret[0].contractRet == "UNKNOWN")
    | [$number, .txID, .ret[0].contractRet]
    | @tsv
  ' <<<"$blocks")

  cursor=$next
done
