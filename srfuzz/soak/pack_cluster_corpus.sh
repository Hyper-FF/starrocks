#!/usr/bin/env bash
# Turn an emitted run directory into a corpus the cluster arm will actually read.
#
# The cluster harness enumerates its groups with `ls "$CORPUS"/*.setup.sql`, strips the suffix, and
# then reads `<group>.query.sql`. It never looks at `.novel.sql`, so handing it an emit directory
# unpacked would run the FULL query set -- every mutant that round-tripped -- and the coverage-novel
# selection would be silently ignored. Same statements, fourteen times the replay cost, and no
# error to say so.
#
# So packing is a rename, not a conversion: the novel statements become the group's query.sql. The
# harness stays untouched, which is the point -- it is 1200 lines that work.
#
#   ./pack_cluster_corpus.sh <emit-run-dir> <dest-dir>
#
# Groups with no novel statements are skipped rather than emitted empty: an empty query.sql is a
# group that runs its setup, loads its data and asks nothing, which costs a full schema build per
# round for no coverage.
set -eu

if [ $# -ne 2 ]; then
    echo "usage: $0 <emit-run-dir> <dest-dir>" >&2
    exit 2
fi

src=$1
dest=$2

if [ ! -d "$src" ]; then
    echo "no such emit directory: $src" >&2
    exit 1
fi

mkdir -p "$dest"
packed=0
skipped=0
statements=0

for setup in "$src"/mut_*.setup.sql; do
    [ -e "$setup" ] || break
    group=${setup%.setup.sql}
    name=$(basename "$group")
    novel="$group.novel.sql"

    if [ ! -s "$novel" ]; then
        skipped=$((skipped + 1))
        continue
    fi
    # Count statements, not lines: the provenance header is `--` comments and would inflate a
    # line count into a claim about coverage that the file does not support.
    n=$(grep -vc '^--' "$novel" || true)
    if [ "$n" -eq 0 ]; then
        skipped=$((skipped + 1))
        continue
    fi

    cp "$setup" "$dest/$name.setup.sql"
    cp "$novel" "$dest/$name.query.sql"
    packed=$((packed + 1))
    statements=$((statements + n))
done

echo "packed=$packed groups ($statements statements) skipped=$skipped -> $dest"

# The lineage travels with the corpus. Without it a packed directory is a pile of SQL whose origin
# lives only in the shell history of whoever ran this.
if [ "$packed" -gt 0 ]; then
    {
        echo "packed-from: $src"
        echo "packed-at: $(date '+%F %T')"
        echo "groups: $packed  statements: $statements  skipped-empty: $skipped"
        echo "selection: coverage-novel mutants only (see srfuzz-origin headers in each file)"
    } > "$dest/PACKED.txt"
fi
