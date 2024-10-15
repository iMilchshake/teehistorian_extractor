use log::info;
use serde_json::to_string;
use std::{fs::File, io::Write, path::PathBuf, process::exit};

use teehistorian_extractor::extractor::{Extractor, SimpleSequence};

fn main() {
    colog::default_builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    // parse
    let sequences = Extractor::get_all_ddnet_sequences(PathBuf::from("./data/random/"));
    info!("extracted {} sequences", sequences.len());

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

    let mut file = File::create("data/out/all_sequences.json").expect("cant create export file");

    info!("converting to simplified sequences");
    let simple_sequences: Vec<SimpleSequence> = sequences
        .iter()
        .map(|ddnet_seq| SimpleSequence::from_ddnet_sequence(ddnet_seq))
        .collect();

    // TODO: this will explode if string is larger than main memory lol (chunk?)
    info!("converting to json");
    let json_data = to_string(&simple_sequences).unwrap();

    info!("writing to file");
    file.write_all(json_data.as_bytes()).unwrap();
    info!("done :)");
}
