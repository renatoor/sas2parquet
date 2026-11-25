"""Baselines for the benchmark: convert a SAS7BDAT file to Parquet with Python tooling.

Usage: python scripts/baseline.py {pandas,pyreadstat,pyreadstat-mp} <input> <output>
"""

import os
import sys

import pandas as pd


def main() -> None:
    tool, src, dst = sys.argv[1:4]
    if tool == "pandas":
        df = pd.read_sas(src, encoding="infer")
    elif tool == "pyreadstat":
        import pyreadstat

        df, _ = pyreadstat.read_sas7bdat(src)
    elif tool == "pyreadstat-mp":
        import pyreadstat

        df, _ = pyreadstat.read_file_multiprocessing(
            pyreadstat.read_sas7bdat, src, num_processes=os.cpu_count()
        )
    else:
        raise SystemExit(f"unknown tool {tool}")
    df.to_parquet(dst, compression="zstd", index=False)


if __name__ == "__main__":
    main()
