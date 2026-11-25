#!/usr/bin/env bash
# Benchmark sas2parquet against pyreadstat and pandas on inflated sample files.
# Needs: hyperfine, python3 with pandas, pyarrow and pyreadstat.
# Usage: scripts/bench.sh <work_dir> [copies]
set -euo pipefail

work=${1:?usage: scripts/bench.sh <work_dir> [copies]}
copies=${2:-20}
root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$work"

cargo build --release --manifest-path "$root/Cargo.toml"
bin="$root/target/release/sas2parquet"

python3 "$root/scripts/inflate.py" "$root/tests/data/numeric_1000000_2.sas7bdat" "$work/numeric.sas7bdat" "$copies"
python3 "$root/scripts/inflate.py" "$root/tests/data/topical.sas7bdat" "$work/topical.sas7bdat" "$copies"

for name in numeric topical; do
  input="$work/$name.sas7bdat"
  hyperfine --warmup 1 --runs 5 --export-markdown "$work/$name-rust.md" \
    -n "sas2parquet" "$bin $input -o $work/out-rust.parquet" \
    -n "sas2parquet -j1" "$bin -j1 $input -o $work/out-rust1.parquet"
  hyperfine --runs 3 --export-markdown "$work/$name-python.md" \
    -n "pyreadstat" "python3 $root/scripts/baseline.py pyreadstat $input $work/out-prs.parquet" \
    -n "pyreadstat (multiprocess)" "python3 $root/scripts/baseline.py pyreadstat-mp $input $work/out-prsmp.parquet" \
    -n "pandas" "python3 $root/scripts/baseline.py pandas $input $work/out-pd.parquet"
done
