#!/usr/bin/env bash
# Run on Linux/WSL with SMT's required Go toolchain on PATH.
# Usage: bash testdata/check-dmt-consumer.sh /path/to/DMT [output-directory]
# DMT is read only; tests/builds run in a disposable archive of its HEAD.
set -euo pipefail
export GOTOOLCHAIN=local
export GOWORK=off
smt_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
dmt_root=$(cd "${1:?usage: check-dmt-consumer.sh DMT-directory [output-directory]}" && pwd)
output=${2:-"$smt_root/.acceptance-artifacts/dmt-consumer/$(date -u +%Y%m%dT%H%M%SZ)"}
mkdir -p "$output"
output=$(cd "$output" && pwd)
work=$(mktemp -d -t smt-dmt-consumer.XXXXXXXX)
trap 'rm -rf -- "$work"' EXIT
git -C "$dmt_root" archive HEAD | tar -x -C "$work"
candidate="$work/smt-candidate"
mkdir "$candidate"
# Freeze tracked and non-ignored untracked candidate files, including local edits.
# This also isolates the run from ongoing implementation in the shared checkout.
(
    cd "$smt_root"
    git ls-files -z --cached --others --exclude-standard | sort -zu |
        while IFS= read -r -d '' file; do
            if [[ -f "$file" ]]; then
                mkdir -p "$candidate/$(dirname "$file")"
                cp -- "$file" "$candidate/$file"
            fi
        done
)
git -C "$dmt_root" status --porcelain=v1 > "$output/dmt-source-status-before.txt"
sha256sum "$dmt_root/go.mod" "$dmt_root/go.sum" > "$output/dmt-dependencies-before.sha256"
{
    printf 'utc=%s\n' "$(date -u +%FT%TZ)"
    printf 'dmt_revision=%s\n' "$(git -C "$dmt_root" rev-parse HEAD)"
    printf 'smt_candidate_revision=%s\n' "$(git -C "$smt_root" rev-parse HEAD)"
    printf 'smt_candidate_path=%s\n' "$smt_root"
    go version
} > "$output/revisions.txt"
git -C "$smt_root" diff --binary HEAD > "$output/smt-candidate.patch"
git -C "$smt_root" ls-files --others --exclude-standard > "$output/smt-candidate-untracked.txt"
sha256sum "$output/smt-candidate.patch" >> "$output/revisions.txt"
(
    cd "$candidate"
    find . -type f -print0 | sort -z | xargs -0 sha256sum
) > "$output/smt-candidate-files.sha256"
sha256sum "$output/smt-candidate-files.sha256" >> "$output/revisions.txt"
cd "$work"
go list -m github.com/johndauphine/smt >> "$output/revisions.txt"
run_gate() {
    local label=$1
    shift
    local status=0
    "$@" > "$output/$label.log" 2>&1 || status=$?
    printf '%s=%s\n' "$label" "$status" | tee -a "$output/outcomes.txt"
    if (( status != 0 )); then tail -60 "$output/$label.log"; fi
    return "$status"
}
# Execute both gates even if a test/build fails, retaining individual outcomes.
failed=0
run_gate baseline-tests go test -short -count=1 ./internal/smtddl ./internal/driver ./internal/driver/generic || failed=1
run_gate baseline-build go build -o "$output/dmt-baseline" ./cmd/dmt || failed=1
cp go.mod candidate.mod
cp go.sum candidate.sum
go mod edit -modfile=candidate.mod -replace="github.com/johndauphine/smt=$candidate"
go list -mod=mod -modfile=candidate.mod -m github.com/johndauphine/smt >> "$output/revisions.txt"
run_gate candidate-tests go test -mod=mod -modfile=candidate.mod -short -count=1 ./internal/smtddl ./internal/driver ./internal/driver/generic || failed=1
run_gate candidate-build go build -mod=mod -modfile=candidate.mod -o "$output/dmt-candidate" ./cmd/dmt || failed=1
git -C "$dmt_root" status --porcelain=v1 > "$output/dmt-source-status-after.txt"
sha256sum "$dmt_root/go.mod" "$dmt_root/go.sum" > "$output/dmt-dependencies-after.sha256"
cmp "$output/dmt-source-status-before.txt" "$output/dmt-source-status-after.txt"
cmp "$output/dmt-dependencies-before.sha256" "$output/dmt-dependencies-after.sha256"
printf 'Evidence: %s\n' "$output"
exit "$failed"
