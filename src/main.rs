use clap::{Parser, ValueEnum};
use memmap2::Mmap;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sas2parquet::encoding::sas_name;
use sas2parquet::{ReadOptions, SasReader, write_parquet};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Codec {
    Zstd,
    Snappy,
    None,
}

/// Convert SAS7BDAT files to Apache Parquet.
#[derive(Debug, Parser)]
#[command(version)]
struct Cli {
    /// Input .sas7bdat file.
    input: PathBuf,

    /// Output .parquet file. Defaults to the input path with a .parquet extension.
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Parquet compression codec.
    #[arg(long, value_enum, default_value_t = Codec::Zstd)]
    compression: Codec,

    /// Rows per record batch handed to the Parquet writer.
    #[arg(long, default_value_t = 64 * 1024)]
    batch_rows: usize,

    /// Override the file's declared encoding (WHATWG label, e.g. "windows-1252").
    #[arg(long)]
    encoding: Option<String>,

    /// Number of decoding threads (defaults to the number of CPUs).
    #[arg(short = 'j', long)]
    threads: Option<usize>,

    /// Print the file's metadata and exit without converting.
    #[arg(long)]
    info: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {}: {err}", cli.input.display());
            ExitCode::FAILURE
        }
    }
}

fn run(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(threads) = cli.threads {
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()?;
    }

    let file = File::open(&cli.input)?;
    // SAFETY: the mapping is read-only. If another process truncates or rewrites the
    // file while we read it the results are undefined, which is the usual mmap caveat
    // for a batch conversion tool.
    let data = unsafe { Mmap::map(&file)? };

    let options = ReadOptions {
        encoding: cli.encoding.clone(),
        batch_rows: cli.batch_rows,
    };
    let reader = SasReader::new(&data, &options)?;

    if cli.info {
        print_info(&reader);
        return Ok(());
    }

    let output = cli
        .output
        .clone()
        .unwrap_or_else(|| cli.input.with_extension("parquet"));
    let start = Instant::now();
    let rows = convert(&reader, &output, cli.compression)?;
    eprintln!(
        "{} -> {}: {rows} rows, {} columns in {:.2?}",
        cli.input.display(),
        output.display(),
        reader.metadata().columns.len(),
        start.elapsed()
    );
    Ok(())
}

/// Writes to a temporary file next to the output and renames it on success, so a
/// failed conversion never leaves a truncated Parquet file behind.
fn convert(
    reader: &SasReader<'_>,
    output: &Path,
    codec: Codec,
) -> Result<usize, Box<dyn std::error::Error>> {
    let compression = match codec {
        Codec::Zstd => Compression::ZSTD(ZstdLevel::default()),
        Codec::Snappy => Compression::SNAPPY,
        Codec::None => Compression::UNCOMPRESSED,
    };
    let properties = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let mut tmp_name = output.as_os_str().to_owned();
    tmp_name.push(".partial");
    let tmp = PathBuf::from(tmp_name);

    let result = File::create(&tmp)
        .map_err(Into::into)
        .and_then(|f| write_parquet(reader, BufWriter::new(f), properties));
    match result {
        Ok(rows) => {
            std::fs::rename(&tmp, output)?;
            Ok(rows)
        }
        Err(err) => {
            let _ = std::fs::remove_file(&tmp);
            Err(err.into())
        }
    }
}

fn print_info(reader: &SasReader<'_>) {
    let header = reader.header();
    let meta = reader.metadata();
    let schema = reader.schema();
    let text = |b: &[u8]| sas2parquet::metadata::decode_padded(reader.decoder(), b);

    println!("dataset:      {}", text(&header.dataset_name));
    println!(
        "sas release:  {} on {}",
        text(&header.sas_release),
        text(&header.host)
    );
    println!(
        "layout:       {}-bit, {:?} endian, {:?}",
        if header.layout.wide { 64 } else { 32 },
        header.layout.endianness,
        header.platform
    );
    println!(
        "encoding:     {} (read as {})",
        sas_name(header.encoding_code),
        reader.decoder().name()
    );
    println!("compression:  {:?}", meta.compression);
    println!(
        "pages:        {} x {} bytes",
        header.page_count, header.page_len
    );
    println!(
        "rows:         {} declared, {} readable",
        meta.row_count,
        reader.row_count()
    );
    println!("row length:   {} bytes", meta.row_length);
    println!("columns:      {}", meta.columns.len());
    for (column, field) in meta.columns.iter().zip(schema.fields()) {
        println!(
            "  {:<32} {:<12} width {:<4} format {:<12} {}",
            column.name,
            field.data_type().to_string(),
            column.width,
            column.format,
            column.label
        );
    }
}
