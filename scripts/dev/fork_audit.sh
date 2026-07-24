#!/usr/bin/env bash
# fork_audit.sh — full proposal/fork-gate inventory for java-tron and go-tron.
#
# This is an evidence generator, not a semantic equivalence prover. It records:
#   1. every active java-tron ProposalType and its go-tron DP-key mapping;
#   2. every java proposal-validation software-version gate;
#   3. every production java call to getAllow*/allow*/support*;
#   4. proposal-aware helper calls hidden outside actuators (capsules/utils);
#   5. the corresponding production go-tron gate callsites.
#
# Usage:
#   JAVA_TRON_DIR=/path/to/java-tron scripts/dev/fork_audit.sh
#
# JAVA_TRON_REV may be supplied when JAVA_TRON_DIR is an exported tree without
# .git metadata. Output is a dated snapshot under docs/dev/.

set -euo pipefail

JAVA_TRON_DIR="${JAVA_TRON_DIR:-/Users/asuka/Projects/tron/java-tron}"
GO_TRON_DIR="${GO_TRON_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
OUT="${GO_TRON_DIR}/docs/dev/fork-audit-$(date -u +%Y-%m-%d).md"

JAVA_PROPOSAL_UTIL="${JAVA_TRON_DIR}/actuator/src/main/java/org/tron/core/utils/ProposalUtil.java"
JAVA_PROPOSAL_SERVICE="${JAVA_TRON_DIR}/framework/src/main/java/org/tron/core/consensus/ProposalService.java"
GO_FORKS="${GO_TRON_DIR}/core/forks/forks.go"

for required in "${JAVA_PROPOSAL_UTIL}" "${JAVA_PROPOSAL_SERVICE}" "${GO_FORKS}"; do
  if [[ ! -f "${required}" ]]; then
    echo "ERROR: required audit source not found: ${required}" >&2
    exit 1
  fi
done

java_rev="${JAVA_TRON_REV:-}"
if [[ -z "${java_rev}" ]]; then
  java_rev=$(git -C "${JAVA_TRON_DIR}" rev-parse HEAD 2>/dev/null || echo unknown)
fi
go_rev=$(git -C "${GO_TRON_DIR}" rev-parse HEAD 2>/dev/null || echo unknown)

emit_calls() {
  local root="$1"
  local pattern="$2"
  shift 2
  while IFS= read -r line; do
    local file line_no body
    file=${line%%:*}
    line_no=${line#*:}
    line_no=${line_no%%:*}
    body=${line#*:*:}
    file=${file#"${root}/"}
    body=$(printf '%s' "${body}" | sed -e 's/|/\\|/g' -e 's/^[[:space:]]*//')
    printf '| `%s` | %s | `%s` |\n' "${file}" "${line_no}" "${body}"
  done < <(rg -n --with-filename --glob '!**/src/test/**' --glob '!**/*_test.go' "${pattern}" "$@" || true)
}

{
  echo '# Fork-Gate Audit — full-tree evidence'
  echo
  echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "- java-tron: \`${java_rev}\`"
  echo "- go-tron: \`${go_rev}\`"
  echo '- Scope: production sources only; Java and Go tests are excluded from callsite inventories.'
  echo '- Interpretation: this file inventories syntactic gates. Semantic parity conclusions belong in the accompanying reviewed audit.'
  echo
  echo '## Proposal universe and mapping'
  echo
  echo '| ID | java `ProposalType` | Validation case | Apply case | go-tron DP key |'
  echo '|---:|---|---:|---:|---|'
  while read -r proposal_id proposal_name; do
    validation_line=$(rg -n "case ${proposal_name}:" "${JAVA_PROPOSAL_UTIL}" | head -1 | cut -d: -f1 || true)
    apply_line=$(rg -n "case ${proposal_name}:" "${JAVA_PROPOSAL_SERVICE}" | head -1 | cut -d: -f1 || true)
    go_key=$(sed -nE "s/^[[:space:]]*${proposal_id}:[[:space:]]*\"([^\"]+)\".*/\\1/p" "${GO_FORKS}" | head -1)
    printf '| %s | `%s` | %s | %s | `%s` |\n' \
      "${proposal_id}" "${proposal_name}" "${validation_line:--}" "${apply_line:--}" "${go_key:-(missing)}"
  done < <(
    sed -n '/public enum ProposalType/,/private long code/p' "${JAVA_PROPOSAL_UTIL}" |
      sed -nE '/^[[:space:]]+[A-Z][A-Z0-9_]+\([0-9]+\)/s/^[[:space:]]+([A-Z][A-Z0-9_]+)\(([0-9]+)\).*/\2 \1/p'
  )
  echo
  echo '## Proposal-validation software-version gates'
  echo
  echo 'Every production `forkController.pass(...)` occurrence in `ProposalUtil.java`:'
  echo
  echo '| File | Line | Check |'
  echo '|---|---:|---|'
  emit_calls "${JAVA_TRON_DIR}" '^[[:space:]]*(if|\|\||&&).*forkController\.pass' "${JAVA_PROPOSAL_UTIL}"
  echo
  echo '## Java production proposal-feature reads'
  echo
  echo 'Every direct production call whose method begins with `getAllow`, `allow`, or `support`.'
  echo 'This deliberately scans the complete Java tree, not only `*Actuator.java`.'
  echo
  echo '| File | Line | Check |'
  echo '|---|---:|---|'
  emit_calls "${JAVA_TRON_DIR}" '\.(getAllow[A-Z][A-Za-z0-9]*|allow[A-Z][A-Za-z0-9]*|support[A-Z][A-Za-z0-9]*)\(' "${JAVA_TRON_DIR}"
  echo
  echo '## Java proposal-aware capsule/helper reads'
  echo
  echo 'These are especially important because callers may invoke a neutral-looking helper'
  echo 'while the helper internally switches consensus behavior by proposal state.'
  echo
  echo '| File | Line | Check |'
  echo '|---|---:|---|'
  emit_calls "${JAVA_TRON_DIR}" '\.(getAllow[A-Z][A-Za-z0-9]*|allow[A-Z][A-Za-z0-9]*|support[A-Z][A-Za-z0-9]*)\(' \
    "${JAVA_TRON_DIR}/chainbase/src/main/java/org/tron/core/capsule" \
    "${JAVA_TRON_DIR}/chainbase/src/main/java/org/tron/common/utils"
  echo
  echo '## go-tron production proposal/fork reads'
  echo
  echo '| File | Line | Check |'
  echo '|---|---:|---|'
  emit_calls "${GO_TRON_DIR}" '(forks\.(IsActive|Pass|PassVersion)[A-Za-z0-9]*|\.(Allow[A-Z][A-Za-z0-9]*|Support[A-Z][A-Za-z0-9]*|ChangeDelegation|ConsensusLogicOptimization|ForbidTransferToContract|UseNewRewardAlgorithm|UnfreezeDelayDays|MaxDelegateLockPeriod)\()' \
    "${GO_TRON_DIR}/actuator" "${GO_TRON_DIR}/consensus" "${GO_TRON_DIR}/core" \
    "${GO_TRON_DIR}/net" "${GO_TRON_DIR}/p2p" "${GO_TRON_DIR}/vm"
} > "${OUT}"

echo "Audit written to ${OUT}"
