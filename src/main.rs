use clap::Parser;
use log::info;
use log::LevelFilter;
use plotlib::page::Page;
use plotlib::repr::Histogram;
use plotlib::repr::HistogramBins;
use plotlib::view::ContinuousView;
use std::fs;
use std::path::PathBuf;
use teehistorian_extractor::export;
use teehistorian_extractor::extractor::{Extractor, SimpleSequence};
use teehistorian_extractor::preprocess;
use teehistorian_extractor::preprocess::Duration;

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

fn log_sequence_info(sequences: &[SimpleSequence]) {
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

    /// Output Arrow file path
    #[clap(short, long, default_value = "./data/out/")]
    output_path: PathBuf,

    /// Minimum ticks per sequence to include
    #[clap(short, long, default_value = "1000")]
    min_ticks: usize,

    /// ticks of no movement that counts as player being AFK
    #[clap(short, long, default_value = "1000")]
    afk_ticks: usize,

    /// ticks of padding around afk durations
    #[clap(short = 'p', long, default_value = "15")]
    afk_padding: usize,

    /// Logging level (error, warn, info, debug, trace)
    #[clap(short, long, default_value = "info")]
    log_level: LevelFilter,

    /// cut sequence on player kill
    #[clap(short = 'k', long)]
    cut_kill: bool,

    /// cut sequence on player rescue (/r)
    #[clap(short = 'r', long)]
    cut_rescue: bool,
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
    let mut ddnet_sequences =
        Extractor::get_all_ddnet_sequences(args.input, args.cut_kill, args.cut_rescue);
    info!("extracted {} ddnet sequences", ddnet_sequences.len());

    // convert so simple sequences
    let mut sequences: Vec<SimpleSequence> = Vec::new();
    while let Some(ddnet_seq) = ddnet_sequences.pop() {
        let simple_seq = SimpleSequence::from_ddnet_sequence(&ddnet_seq);

        if simple_seq.tick_count > args.min_ticks {
            sequences.push(simple_seq);
        }
    }

    info!("converted to {} simple sequences", sequences.len());
    log_sequence_info(&sequences);

    // determine total tick count
    // export_to_dir(&simple_sequences, &args.output_path); info!("Arrow data written to {:?}", &args.output_path);

    let extracted_sequences: Vec<SimpleSequence> = sequences
        .iter()
        .flat_map(|sequence| {
            let durations = Duration::get_non_afk_durations(sequence, args.afk_ticks);
            let durations =
                Duration::pad_durations(durations, sequence.tick_count, args.afk_padding);
            let durations: Vec<Duration> = durations
                .iter()
                .flat_map(|duration| duration.cut_duration(args.min_ticks))
                .collect();
            Duration::extract_sub_sequences(sequence, durations)
        })
        .collect();

    info!("extracted gameplay sequences:");
    log_sequence_info(&extracted_sequences);

    for (player, seq_count) in preprocess::get_top_k_players(&extracted_sequences, 20, false) {
        println!("{:15}: {}", player, seq_count,);
    }

    for (player, ticks) in preprocess::get_top_k_players(&extracted_sequences, 20, true) {
        println!("{:15}: {:.1}h", player, ticks as f32 / (50. * 60. * 60.));
    }

    export::convert_and_save_sequences_to_npz(&extracted_sequences, "test.npz");
    info!("exported as tensor!");

    Ok(())
}
