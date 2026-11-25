"""Compare sas2parquet output against pandas.read_sas for every file in a directory.

Usage: python scripts/compare.py <sas_dir> <parquet_dir>
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


# encoding_rs implements the WHATWG Encoding Standard, which differs from Python's codecs
# for a few labels. Decode pandas' raw bytes with the Python codec that matches.
WHATWG_UNDEFINED_1252 = {0x81, 0x8D, 0x8F, 0x90, 0x9D}


def whatwg_decoder(encoding: str):
    name = encoding.lower()
    if name == "windows-1252":
        # WHATWG maps the five bytes cp1252 leaves undefined to the matching C1 controls.
        return lambda b: "".join(
            chr(c) if c in WHATWG_UNDEFINED_1252 else bytes([c]).decode("cp1252") for c in b
        )
    codec = {"shift_jis": "cp932", "euc-kr": "cp949", "big5": "big5hkscs"}.get(name, name)
    return lambda b: b.decode(codec)


def normalize(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return series.astype("datetime64[ms]")
    if series.dtype == object:
        # pandas turns blank strings into NaN; sas2parquet writes them as null.
        return series.where(series.notna(), "").astype(str)
    return series


def compare(sas_path: Path, parquet_path: Path) -> list[str]:
    table = pq.read_table(parquet_path)
    encoding = (table.schema.metadata or {}).get(b"sas.encoding", b"utf-8").decode()
    ours = table.to_pandas(date_as_object=False)
    try:
        theirs = pd.read_sas(sas_path, encoding=None)
    except pd.errors.EmptyDataError:
        # pandas cannot represent a dataset without columns.
        return [] if table.num_columns == 0 else ["pandas found no columns"]

    decode = whatwg_decoder(encoding)
    theirs.columns = [c.decode("latin-1") if isinstance(c, bytes) else c for c in theirs.columns]
    for name in theirs.columns:
        if theirs[name].dtype == object:
            theirs[name] = theirs[name].map(lambda v: decode(v) if isinstance(v, bytes) else v)

    problems = []
    if list(ours.columns) != list(theirs.columns):
        problems.append(f"columns differ: {list(ours.columns)[:5]} vs {list(theirs.columns)[:5]}")
        return problems
    if len(ours) != len(theirs):
        problems.append(f"row count {len(ours)} vs pandas {len(theirs)}")
        return problems

    for name in ours.columns:
        a, b = normalize(ours[name]), normalize(theirs[name])
        if ours[name].isna().all() and theirs[name].isna().all():
            same = True  # e.g. an all-blank text column, which pandas turns into float NaN
        elif a.dtype.kind == "f" and b.dtype.kind == "f":
            same = np.array_equal(a.to_numpy(), b.to_numpy(), equal_nan=True)
        else:
            same = a.reset_index(drop=True).equals(b.reset_index(drop=True))
        if not same:
            diff = (a != b) & ~(a.isna() & b.isna())
            i = int(np.argmax(diff.to_numpy())) if diff.any() else 0
            problems.append(
                f"column {name!r} ({ours[name].dtype} vs {theirs[name].dtype}): "
                f"row {i}: {a.iloc[i]!r} vs {b.iloc[i]!r}"
            )
    return problems


def main() -> int:
    sas_dir, parquet_dir = Path(sys.argv[1]), Path(sys.argv[2])
    failures = 0
    for sas_path in sorted(sas_dir.glob("*.sas7bdat")):
        parquet_path = parquet_dir / (sas_path.stem + ".parquet")
        try:
            problems = compare(sas_path, parquet_path)
        except Exception as exc:  # noqa: BLE001 - report and keep going
            problems = [f"{type(exc).__name__}: {exc}"]
        status = "ok" if not problems else "MISMATCH"
        print(f"{sas_path.name:<28} {status}")
        for problem in problems[:5]:
            print(f"    {problem}")
        failures += bool(problems)
    print(f"\n{failures} file(s) with differences")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
