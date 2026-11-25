"""Write tests/expected.json: a per-column fingerprint of every converted sample file.

Run it only on output that scripts/compare.py has verified against pandas:
    python scripts/golden.py <parquet_dir> tests/expected.json
"""

import json
import math
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def fingerprint(column: pa.ChunkedArray) -> dict:
    valid = pc.drop_null(column)
    entry = {"type": str(column.type), "nulls": column.null_count}
    if pa.types.is_string(column.type):
        values = valid.to_pylist()
        entry["chars"] = sum(len(v) for v in values)
        entry["first"] = values[0] if values else None
        entry["last"] = values[-1] if values else None
    else:
        if pa.types.is_floating(column.type):
            numbers = valid.to_pylist()
        else:  # date32 and timestamp: compare the underlying integers
            storage = pa.int32() if pa.types.is_date32(column.type) else pa.int64()
            numbers = valid.cast(storage).to_pylist()
        entry["sum"] = math.fsum(numbers)
    return entry


def main() -> None:
    parquet_dir, out = Path(sys.argv[1]), Path(sys.argv[2])
    expected = {}
    for path in sorted(parquet_dir.glob("*.parquet")):
        table = pq.read_table(path)
        expected[path.stem] = {
            "rows": table.num_rows,
            "columns": {name: fingerprint(table[name]) for name in table.column_names},
        }
    out.write_text(json.dumps(expected, indent=1, ensure_ascii=False) + "\n")
    print(f"wrote {len(expected)} files to {out}")


if __name__ == "__main__":
    main()
