use clap::Parser;
use log::info;
use log::LevelFilter;
use log::{debug, warn};
use plotlib::page::Page;
use plotlib::repr::Histogram;
use plotlib::repr::HistogramBins;
use plotlib::view::ContinuousView;
use std::fs;
use std::path::PathBuf;
use teehistorian_extractor::export::Exporter;
use teehistorian_extractor::extractor::{Extractor, Sequence};
use teehistorian_extractor::preprocess::Duration;

fn plot(sequences: &[Sequence], title: &str) {
    let tick_counts: Vec<f64> = sequences
        .iter()
        .map(|s| s.tick_count as f64)
        .filter(|&ticks| ticks < 10000.0)
        .collect();
    let hist = Histogram::from_slice(&tick_counts, HistogramBins::Count(100));
    let view = ContinuousView::new()
        .add(hist)
        .x_label("Tick Count")
        .y_label("Frequency");
    Page::single(&view)
        .save(format!("histogram_{}.svg", title))
        .expect("Failed to save plot");
}

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
    #[clap(short, long, default_value = "1000")]
    afk_ticks: usize,

    /// Ticks of padding around afk durations
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
}

fn batched_export(args: &Cli) {
    // start with initializing output dataset, in case it fails
    let mut exporter = Exporter::new(&args.output_folder, args.seq_length, 4);
    assert!(
        args.output_folder.is_dir(),
        "Output path is not a directory"
    );

    // get all files
    let paths: Vec<_> = fs::read_dir(&args.input)
        .unwrap()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect();
    info!("found {} files to parse", paths.len());

    // process all files in batches
    let batch_size = 1000;
    for batch in paths.chunks(batch_size) {
        // parse batch -> DDNetSequences
        let mut sequence_batch = Vec::new();
        for path in batch {
            let x = Extractor::get_ddnet_sequences(&path, args.cut_kill, args.cut_rescue);
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
                    Duration::pad_durations(durations, sequence.tick_count, args.afk_padding);
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

    // dbg!(&exporter.sequence_count);
    // dbg!(&exporter.player_count);
    // dbg!(&exporter.players);

    info!("exported as tensor!");

    Ok(())
}
