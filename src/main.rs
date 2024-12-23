use clap::Parser;
use log::info;
use log::LevelFilter;
use std::fs;
use std::path::PathBuf;
use teehistorian_extractor::export::ExportConfig;
use teehistorian_extractor::export::Exporter;
use teehistorian_extractor::extractor::{Extractor, Sequence};
use teehistorian_extractor::parser::ParserConfig;
use teehistorian_extractor::preprocess::Duration;

fn log_sequence_info(sequences: &[Sequence]) {
    let total_ticks = sequences.iter().map(|s| s.tick_count).sum::<usize>();
    info!(
        "sequences={}, ticks={} => {:.1} hours of gameplay",
        sequences.len(),
        total_ticks,
        (total_ticks as f32 / (50. * 60. * 60.))
    );
}

#[derive(Parser, Debug)]
struct Cli {
    /// Input data directory
    #[clap(short, long, default_value = "./data/teehistorian/")]
    input: PathBuf,

    /// Filepath for output dataset folder
    #[clap(short, long, default_value = "./data/out/dataset/")]
    output_folder: PathBuf,

    /// ticks per sequence
    #[clap(short, long, default_value = "1000")]
    seq_length: usize,

    /// Ticks of no movement that counts as player being AFK
    #[clap(short, long, default_value = "500")]
    afk_ticks: usize,

    /// Ticks of padding around durations, after afk removal
    #[clap(short = 'p', long, default_value = "15")]
    afk_padding: usize,

    /// Logging level (error, warn, info, debug, trace)
    #[clap(short, long, default_value = "info")]
    log_level: LevelFilter,

    /// Cut sequence on player kill
    #[clap(short = 'k', long)]
    cut_kill: bool,

    /// Cut sequence on player rescue (/r)
    #[clap(short = 'r', long)]
    cut_rescue: bool,

    /// number of teehistorian files to process before saving to file
    #[clap(short = 'b', long, default_value = "1000")]
    file_chunk_size: usize,

    /// number of teehistorian files to process before saving to file
    #[clap(long, default_value = "2000")]
    max_files: usize,

    #[clap(long, default_value = "100")]
    max_speed: i32,

    #[clap(long, default_value = "1000")]
    max_aim_distance: i32,
}

fn batched_export(args: &Cli) {
    // start with initializing output dataset, in case it fails
    assert!(
        args.output_folder.is_dir(),
        "Output path is not a directory"
    );

    let parser_config = ParserConfig::new(args.cut_kill, args.cut_rescue, args.max_speed);
    let export_config = ExportConfig::new(args.seq_length, true, false, true, true);
    let mut exporter = Exporter::new(&args.output_folder, export_config);

    // get all files
    let mut paths: Vec<_> = fs::read_dir(&args.input)
        .unwrap()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect();
    paths.truncate(args.max_files);
    let file_count = paths.len();
    let chunk_count = (file_count + args.file_chunk_size - 1) / args.file_chunk_size;
    info!("found {} files to parse", file_count);

    // process all files in chunks
    for (chunk_index, chunk) in paths.chunks(args.file_chunk_size).enumerate() {
        info!(
            "[{}/{}] parsing {} files",
            chunk_index + 1,
            chunk_count,
            chunk.len()
        );

        // parse batch -> DDNetSequences
        let mut sequence_batch = Vec::new();
        for path in chunk {
            let x = Extractor::get_ddnet_sequences(&path, &parser_config);
            sequence_batch.extend(x);
        }
        info!("extracted {} ddnet sequences", sequence_batch.len());

        // Convert DDNetSequence -> Sequence
        let mut sequences: Vec<Sequence> = Vec::new();
        while let Some(ddnet_seq) = sequence_batch.pop() {
            let sequence = Sequence::from_ddnet_sequence(&ddnet_seq);

            if sequence.tick_count > args.seq_length {
                sequences.push(sequence);
            }
        }
        info!("converted to {} sequences", sequences.len());
        log_sequence_info(&sequences);

        // Clean sequences
        let cleaned_sequences: Vec<Sequence> = sequences
            .iter()
            .flat_map(|sequence| {
                let durations = Duration::get_non_afk_durations(sequence, args.afk_ticks);
                let durations =
                    Duration::pad_durations(durations, sequence.tick_count - 1, args.afk_padding);
                let durations: Vec<Duration> = durations
                    .iter()
                    .flat_map(|duration| duration.cut_duration(args.seq_length))
                    .collect();
                Duration::extract_sub_sequences(sequence, durations)
            })
            .collect();
        info!("cleaned gameplay sequences:");
        log_sequence_info(&cleaned_sequences);

        // Export batch
        exporter.add_to_dataset(&cleaned_sequences);
        info!("exported batch");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    colog::default_builder()
        .filter_level(args.log_level)
        .target(env_logger::Target::Stdout)
        .init();
    batched_export(&args);
    info!("done");
    Ok(())
}
