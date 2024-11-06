use clap::Parser;
use log::info;
use log::LevelFilter;
use std::fs;
use std::path::PathBuf;
use teehistorian_extractor::export;
use teehistorian_extractor::extractor::{Extractor, SimpleSequence};
use teehistorian_extractor::preprocess;

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
    let sequences = Extractor::get_all_ddnet_sequences(args.input);
    info!("extracted {} sequences", sequences.len());

    // Convert to simplified sequences
    let simple_sequences: Vec<SimpleSequence> = sequences
        .iter()
        .map(SimpleSequence::from_ddnet_sequence)
        .filter(|seq| seq.tick_count > args.min_ticks)
        .collect();
    info!("extracted {} sequences", sequences.len());

    // export::convert_and_save_sequences_to_npz(&simple_sequences, "test.npz");
    // info!("exported as tensor!");

    // determine total tick count
    let total_ticks = simple_sequences.iter().map(|s| s.tick_count).sum::<usize>();
    info!(
        "total ticks={} equal to {:.1} minutes or {:.1} hours of gameplay",
        total_ticks,
        (total_ticks as f32 / (50. * 60.)),
        (total_ticks as f32 / (50. * 60. * 60.))
    );

    // export_to_dir(&simple_sequences, &args.output_path);
    // info!("Arrow data written to {:?}", &args.output_path);

    let top_k = preprocess::get_top_k_players(&simple_sequences, 40);

    for (player, ticks) in top_k {
        println!(
            "{:15}: {} ticks => {:.1} hours",
            player,
            ticks,
            (ticks as f32 / (50. * 60. * 60.))
        );
    }

    for sequence in simple_sequences.iter().take(10) {
        let non_afk = preprocess::get_non_afk_durations(sequence, 400);
        println!("non-afk durations: {:?}", non_afk);
        println!("Full move_dir sequence: {:?}", sequence.move_dir);

        for &(start, end) in &non_afk {
            let subsequence = &sequence.move_dir[start..=end];
            println!(
                "Active subsequence from {} to {}: {:?}",
                start, end, subsequence
            );
        }
    }

    Ok(())
}
