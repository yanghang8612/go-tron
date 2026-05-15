#!/usr/bin/env bash
# Regenerate the bit-exact Java `StrictMath.pow` oracle:
#   1. Run gen_inputs.go to produce inputs.txt.
#   2. Compile Oracle.java.
#   3. Pipe inputs.txt through `java Oracle` to produce expected.txt.
# Both inputs.txt and expected.txt are committed so the Go test is
# hermetic (no Java required at `go test` time). Re-run this script
# only when the vector intentionally changes.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

go run gen_inputs.go > oracle/inputs.txt

javac oracle/Oracle.java
java -cp oracle Oracle < oracle/inputs.txt > oracle/expected.txt
# Drop the generated .class file — not part of the committed test vector.
rm -f oracle/Oracle.class

echo "wrote $(wc -l < oracle/inputs.txt) input pairs and $(wc -l < oracle/expected.txt) expected bit patterns"
