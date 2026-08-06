#!/usr/bin/env bash
# Stamp harvested corpus files as generation 0.
#
# Marking only the fuzzer's own output would leave "no marker" meaning three different things:
# harvested, generated-but-stripped-by-a-copy, and hand-written. The fuzzer reports unstamped files
# as `unstamped` and explicitly not as production, so the count only becomes meaningful once the
# corpora that ARE production say so.
#
# Idempotent: a file that already carries a stamp is left exactly as it is, including its generation.
# Re-stamping a generated file as harvested is the one thing this must never do -- that would launder
# fuzzer-invented shapes into the production count, which is the number the whole scheme protects.
#
#   ./stamp_origin.sh <root-name> <file-or-dir>...
#
# The root name travels down every generation unchanged, so pick the thing the ancestry should be
# attributed to -- the cluster or campaign the shapes were harvested from, not the file.
set -eu

if [ $# -lt 2 ]; then
    echo "usage: $0 <root-name> <file-or-dir>..." >&2
    exit 2
fi

root=$1
shift
stamped=0
skipped=0

stamp_one() {
    f=$1
    if grep -qE '^-- srfuzz-(origin|generation):' "$f"; then
        skipped=$((skipped + 1))
        return
    fi
    tmp=$(mktemp)
    {
        printf -- '-- srfuzz-origin: harvested\n'
        printf -- '-- srfuzz-generation: 0\n'
        printf -- '-- srfuzz-root: %s\n' "$root"
        cat "$f"
    } > "$tmp"
    # Preserve the original mode; corpora are read by several harnesses and one of them runs as root.
    chmod --reference="$f" "$tmp" 2>/dev/null || true
    mv "$tmp" "$f"
    stamped=$((stamped + 1))
}

for target in "$@"; do
    if [ -d "$target" ]; then
        while IFS= read -r f; do
            stamp_one "$f"
        done < <(find "$target" -type f -name '*.sql' | sort)
    else
        stamp_one "$target"
    fi
done

echo "stamped=$stamped already-stamped=$skipped root=$root"
