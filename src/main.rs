use clap::Parser;
use log::info;
use log::LevelFilter;
use plotlib::page::Page;
use plotlib::repr::Histogram;
use plotlib::repr::HistogramBins;
use plotlib::view::ContinuousView;
use std::fs;
use std::path::PathBuf;
use teehistorian_extractor::extractor::{Extractor, SimpleSequence};
use teehistorian_extractor::preprocess;

fn plot(sequences: &[SimpleSequence], title: &str) {
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

#[derive(Parser, Debug)]
struct Cli {
    /// Input data directory
    #[clap(short, long, default_value = "./data/teehistorian/")]
    input: PathBuf,

    /// Output Arrow file path
    #[clap(short, long, default_value = "./data/out/")]
    output_path: PathBuf,

    /// Minimum ticks per sequence to include
    #[clap(short, long, default_value = "100")]
    min_ticks: usize,

    /// Logging level (error, warn, info, debug, trace)
    #[clap(short, long, default_value = "info")]
    log_level: LevelFilter,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    // ensure output_path is a folder and exists
    fs::create_dir_all(&args.output_path).expect("Failed to create directory");
    assert!(args.output_path.is_dir(), "Output path is not a directory");

    colog::default_builder()
        .filter_level(args.log_level)
        .target(env_logger::Target::Stdout)
        .init();

    // Parse sequences
    let ddnet_sequences = Extractor::get_all_ddnet_sequences(args.input);
    info!("extracted {} sequences", ddnet_sequences.len());

    // Convert to simplified sequences
    let sequences: Vec<SimpleSequence> = ddnet_sequences
        .iter()
        .map(SimpleSequence::from_ddnet_sequence)
        .collect();
    info!("extracted {} sequences", ddnet_sequences.len());

    // export::convert_and_save_sequences_to_npz(&simple_sequences, "test.npz");
    // info!("exported as tensor!");

    // determine total tick count
    let total_ticks = sequences.iter().map(|s| s.tick_count).sum::<usize>();
    info!(
        "total ticks={} equal to {:.1} minutes or {:.1} hours of gameplay",
        total_ticks,
        (total_ticks as f32 / (50. * 60.)),
        (total_ticks as f32 / (50. * 60. * 60.))
    );

    // export_to_dir(&simple_sequences, &args.output_path);
    // info!("Arrow data written to {:?}", &args.output_path);

    for (player, seq_count) in preprocess::get_top_k_players(&sequences, 20, false) {
        println!("{:15}: {}", player, seq_count,);
    }

    for (player, ticks) in preprocess::get_top_k_players(&sequences, 20, true) {
        println!("{:15}: {:.1}h", player, ticks as f32 / (50. * 60. * 60.));
    }

    let extracted_sequences: Vec<SimpleSequence> = sequences
        .iter()
        .flat_map(|sequence| {
            let durations = preprocess::get_non_afk_durations(sequence, 1000);
            let padded_durations = preprocess::pad_durations(durations, sequence.tick_count - 1, 5);
            preprocess::extract_sub_sequences(sequence, padded_durations)
        })
        //.filter(|seq| seq.tick_count > args.min_ticks)
        .collect();

    for (player, seq_count) in preprocess::get_top_k_players(&extracted_sequences, 20, false) {
        println!("{:15}: {}", player, seq_count,);
    }

    for (player, ticks) in preprocess::get_top_k_players(&extracted_sequences, 20, true) {
        println!("{:15}: {:.1}h", player, ticks as f32 / (50. * 60. * 60.));
    }

    plot(&sequences, "before_afk");
    plot(&extracted_sequences, "after_afk");

    // TODO: maybe not split on rescue/kill ??

    Ok(())
}
