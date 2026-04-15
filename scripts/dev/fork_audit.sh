#!/usr/bin/env bash
# fork_audit.sh — Fork-gate parity audit between java-tron and go-tron.
#
# Walks java-tron actuator + execution-path sources for forkController.pass
# and support*() checks, walks go-tron actuator + VM sources for
# forks.IsActive / forks.Pass / fc.IsActive calls, then emits a markdown
# report to docs/dev/fork-audit-<date>.md.
#
# Usage:
#   JAVA_TRON_DIR=/path/to/java-tron scripts/dev/fork_audit.sh
#
# Output is intentionally one-shot — freeze the report, don't run in CI.
#
# M1.3 Task 5: only execution-path gates are actionable now; proposal-
# validation gates (ProposalUtil.java) are deferred to M4 when go-tron
# exposes a proposal-create endpoint.

set -euo pipefail

JAVA_TRON_DIR="${JAVA_TRON_DIR:-/Users/asuka/Projects/tron/java-tron}"
GO_TRON_DIR="${GO_TRON_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
OUT="${GO_TRON_DIR}/docs/dev/fork-audit-$(date -u +%Y-%m-%d).md"

if [[ ! -d "${JAVA_TRON_DIR}" ]]; then
  echo "ERROR: java-tron not found at ${JAVA_TRON_DIR}" >&2
  exit 1
fi

{
  echo "# Fork-Gate Audit"
  echo
  echo "Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "java-tron:  ${JAVA_TRON_DIR}"
  echo "go-tron:    ${GO_TRON_DIR}"
  echo
  echo "## (a) Execution-path gates in java-tron actuators"
  echo
  echo "Source: \`actuator/src/main/java/org/tron/core/actuator/*Actuator.java\`"
  echo
  echo '| File | Line | Check |'
  echo '|---|---|---|'
  while IFS= read -r line; do
    f=$(echo "$line" | cut -d: -f1)
    n=$(echo "$line" | cut -d: -f2)
    body=$(echo "$line" | cut -d: -f3- | sed -e 's/|/\\|/g' -e 's/^[[:space:]]*//')
    printf '| %s | %s | `%s` |\n' "$(basename "$f")" "$n" "$body"
  done < <(
    grep -rn --include='*.java' 'forkController\.pass\|dynamicStore\.allow\|dynamicStore\.support\|dynamicPropertiesStore\.allow\|dynamicPropertiesStore\.support' \
      "${JAVA_TRON_DIR}/actuator/src/main/java/org/tron/core/actuator/" 2>/dev/null || true
  )
  echo
  echo "## (b) Execution-path gates in go-tron actuators / VM"
  echo
  echo "Source: \`actuator/*.go\`, \`vm/*.go\`, \`core/*.go\`"
  echo
  echo '| File | Line | Check |'
  echo '|---|---|---|'
  while IFS= read -r line; do
    f=$(echo "$line" | cut -d: -f1 | sed "s|${GO_TRON_DIR}/||")
    n=$(echo "$line" | cut -d: -f2)
    body=$(echo "$line" | cut -d: -f3- | sed -e 's/|/\\|/g' -e 's/^[[:space:]]*//')
    printf '| %s | %s | `%s` |\n' "$f" "$n" "$body"
  done < <(
    grep -rn --include='*.go' 'forks\.IsActive\|forks\.Pass\|\.IsActive(forks\.\|ctx\.ForkController\|dp\.AllowNewResourceModel\|DynProps\.Allow' \
      "${GO_TRON_DIR}/actuator/" "${GO_TRON_DIR}/vm/" "${GO_TRON_DIR}/core/" 2>/dev/null | grep -v _test.go || true
  )
  echo
  echo "## Proposal-validation gates (deferred — M4)"
  echo
  echo "java-tron's ProposalUtil.java contains ~61 \`forkController.pass\` calls"
  echo "governing which proposals may be submitted given the current software"
  echo "version. These are NOT execution-path gates; they only matter once"
  echo "go-tron exposes \`/wallet/createproposal\`. Count (for backlog):"
  cnt=$(grep -c 'forkController\.pass' "${JAVA_TRON_DIR}/actuator/src/main/java/org/tron/core/utils/ProposalUtil.java" 2>/dev/null || echo 0)
  echo
  echo "- ProposalUtil.java: ${cnt} pass() callsites"
} > "${OUT}"

echo "Audit written to ${OUT}"
