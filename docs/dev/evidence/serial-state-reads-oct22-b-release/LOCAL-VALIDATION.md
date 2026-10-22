# B-only release validation plan

This release contains the frozen Bv2 production patch and three B tests on
actual parent 9030ce7a31016491ece014ac76e5656ec149417e. A1 and A2 Darwin
NO-GO records remain unchanged. Any Linux A2 packet included here is inactive
text for a separate B-only versus B+A2 evaluation, not approved production.

The index receives the exact frozen patch with `git apply --cached`; shared
buffer.go is never staged from the combined working file. Explicit B tests,
B specs/evidence, this release plan/runner, and the frozen Linux packet are the
only additional paths. A manifest enumerates every staged file and SHA256.
The final B commit must have this actual parent, exact B production blob
hashes, and no changes outside that manifest. The combined working files and
main WIP remain untouched.

After local commit creation, run `validate-local.py --commit FULL_SHA` once.
It uses the shared benchmark lock, checks the fixed Go/native/header hashes,
then makes a fresh real Git clone at that exact commit. It initializes its
native submodule only from the existing local Git repository at the unchanged
Gitlink commit and copies the fixed existing arm64 static archive into the
ignored target directory. It runs no Cargo command or network fetch. A clean
Git status is required before and after validation.

The serial checks are CGO1 full Sapling `go test -tags=sapling ./... -count=1
-timeout=900s`, full blockbuffer race with the same Sapling tag, and Sapling
`go build -buildvcs=true ./cmd/gtron`. All raw output, exit status, exact Git
identity, native inputs before/after, and binary build metadata are saved
outside the source checkout under ignored build/benchmarks. A failure stops
validation and remains evidence; this does not resample any performance gate.

This local commit/test authorization does not authorize push, server changes,
service changes, deployment locks, or deployment. Astra reviews real Git
objects and the validation logs; Root decides further release actions.
