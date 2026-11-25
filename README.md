# sas2parquet

A fast SAS7BDAT to Apache Parquet converter, written in Rust.

SAS7BDAT is SAS's undocumented binary dataset format. I wrote this while working on a
migration of legacy SAS workloads to a Parquet-based data platform, to learn how the
format works and to see how fast a conversion could be with a zero-copy, parallel design.

```console
$ sas2parquet topical.sas7bdat
topical.sas7bdat -> topical.parquet: 1769435 rows, 114 columns in 1.49s
```

## Features

- **Zero-copy reads.** The file is memory-mapped; uncompressed rows are decoded straight
  from the mapping into Arrow arrays, with no intermediate row buffers.
- **Parallel decoding.** A cheap first pass maps where every row lives; batches of pages
  are then decoded on all cores with rayon and written to Parquet in file order, with
  bounded memory.
- **Compressed files.** Both SAS compression schemes are supported: RLE (`COMPRESS=CHAR`)
  and RDC (`COMPRESS=BINARY`). Compressed rows are expanded into one reusable arena per batch.
- **32/64-bit, little/big-endian** files, truncated numerics (3 to 7 bytes), and the
  character encodings that have a WHATWG equivalent (`--encoding` overrides the header).
- **Types.** Numerics become `Float64`; columns with SAS date and datetime formats become
  `Date32` and `Timestamp(ms)`. SAS missing values and blank strings become nulls. Column
  labels and formats are kept as Parquet field metadata.
- **Robust to bad input.** Every read is bounds-checked: corrupt or truncated files return
  an error instead of panicking. A test flips bytes across sample files to check this.

## Usage

```console
$ cargo install --path .
$ sas2parquet input.sas7bdat                     # writes input.parquet
$ sas2parquet input.sas7bdat -o out.parquet --compression snappy
$ sas2parquet input.sas7bdat --info              # print metadata only
```

Run `sas2parquet --help` for all options. The crate is also a library: `SasReader`
yields Arrow `RecordBatch`es and `write_parquet` does the parallel conversion.

## Correctness

All 37 sample files in `tests/data` (uncompressed, RLE and RDC; 32/64-bit layouts; both
endiannesses; several encodings) are compared value by value against `pandas.read_sas` with `scripts/compare.py`. The verified
results are stored as fingerprints in `tests/expected.json`, which `cargo test` checks.

## Performance

Converting to zstd-compressed Parquet on an Apple M1 Pro (8 cores, 16 GB), measured with
[hyperfine](https://github.com/sharkdp/hyperfine) (`scripts/bench.sh`). The Python
baselines read with `pandas.read_sas` or `pyreadstat` (readstat's C parser) and write with
pyarrow.

| File | sas2parquet | sas2parquet `-j1` | pyreadstat | pyreadstat (8 procs) | pandas |
| --- | ---: | ---: | ---: | ---: | ---: |
| numeric: 323 MB, 20.9M rows x 2 numeric columns, uncompressed | **0.18 s** | 0.57 s | 4.29 s | 3.11 s | 2.32 s |
| topical: 270 MB, 1.77M rows x 114 mixed columns, RLE | **1.45 s** | 5.65 s | 28.0 s | 13.4 s | 33.8 s |

Peak memory on `topical`: 0.9 GB for sas2parquet vs 3.4 to 3.6 GB for the Python tools.

The benchmark files are sample files from `tests/data` with their row pages repeated 20
times (`scripts/inflate.py`); both still match `pandas.read_sas` value for value.

Profiling drove the design: decoding all of `topical` takes 1.7 s on 8 threads
(`examples/decode_only.rs`), but a single-threaded `ArrowWriter` then spent about 6 s
encoding and compressing. Encoding each column chunk on the thread pool brought the
conversion from 8.5 s to 1.45 s.

## How it works

| Module | Responsibility |
| --- | --- |
| `header` | Fixed header: 32/64-bit layout, endianness, encoding, page size and count |
| `page` | Page types and the subheader pointer table on each page |
| `metadata` | Row size, columns, names, labels and formats from the metadata subheaders |
| `decompress` | RLE and RDC decoders that write into a caller-provided row buffer |
| `reader` | Builds the row plan and decodes batches into Arrow arrays |
| `writer` | Parallel batch decoding feeding a Parquet `ArrowWriter` |

The format notes come from the reverse-engineering work in
[readstat](https://github.com/WizardMac/ReadStat) and
[pandas](https://github.com/pandas-dev/pandas/tree/main/pandas/io/sas). `cpp/` holds an
earlier C++ prototype built on readstat.

## Limitations

- Encodings without a WHATWG equivalent (for example CP437 or EUC-TW) are rejected unless
  `--encoding` names a substitute.
- SAS time formats (`TIME`, `HHMM`) stay as numbers of seconds.
- Deleted-row markers on amended datasets are not interpreted.
