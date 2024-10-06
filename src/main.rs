use log::info;
use std::{fs::File, path::PathBuf};

use teehistorian_extractor::extractor::Extractor;

fn main() {
    colog::default_builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    // parse
    let sequences = Extractor::get_sequences(PathBuf::from("./data/random/"));
    info!("extracted {} sequences", sequences.len());

    // export
    for (seq_index, sequence) in sequences.iter().enumerate() {
        info!("{:?}", &sequence);
        let file = File::create(format!("data/out/output_{}.json", seq_index)).unwrap();
        serde_json::to_writer(file, &sequence).unwrap();
    }

    // summary
    let total_ticks = sequences
        .iter()
        .map(|s| s.end_tick.unwrap() - s.start_tick)
        .sum::<i32>();
    info!(
        "total ticks={} equal to {:.1} minutes or {:.1} hours of gameplay",
        total_ticks,
        (total_ticks as f32 / (50. * 60.)),
        (total_ticks as f32 / (50. * 60. * 60.))
    );
}
