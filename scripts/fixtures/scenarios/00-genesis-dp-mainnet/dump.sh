#!/usr/bin/env bash
# Args: <fixture_output_path> <config_path> <http_port>
set -euo pipefail

_here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../lib/api.sh
source "$_here/../../lib/api.sh"
# shellcheck source=../../lib/dump.sh
source "$_here/../../lib/dump.sh"

dump_fixture "$1" "00-genesis-dp-mainnet" "$2" "$3" --section dp
