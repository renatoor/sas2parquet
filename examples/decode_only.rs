//! Times decoding alone (no Parquet), sequentially and in parallel, to separate the
//! reader's cost from the writer's.
//! Usage: cargo run --release --example decode_only -- <file.sas7bdat>

use rayon::prelude::*;
use sas2parquet::{ReadOptions, SasReader};
use std::time::Instant;

fn main() {
    let path = std::env::args().nth(1).expect("usage: decode_only <file>");
    let file = std::fs::File::open(path).unwrap();
    // SAFETY: read-only mapping of a file nobody else is writing.
    let data = unsafe { memmap2::Mmap::map(&file).unwrap() };
    let reader = SasReader::new(&data, &ReadOptions::default()).unwrap();

    let start = Instant::now();
    let rows: usize = (0..reader.num_batches())
        .map(|i| reader.read_batch(i).unwrap().num_rows())
        .sum();
    println!("sequential decode: {rows} rows in {:.2?}", start.elapsed());

    let start = Instant::now();
    let rows: usize = (0..reader.num_batches())
        .into_par_iter()
        .map(|i| reader.read_batch(i).unwrap().num_rows())
        .sum();
    println!("parallel decode:   {rows} rows in {:.2?}", start.elapsed());
}
