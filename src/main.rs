use log::info;
use rmp_serde::to_vec as to_msgpack;
use serde_json::to_string;
use std::{fs::File, io::Write, path::PathBuf, process::exit, time::Instant};

use teehistorian_extractor::extractor::{Extractor, SimpleSequence};

fn main() {
    colog::default_builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    // parse
    let sequences = Extractor::get_all_ddnet_sequences(PathBuf::from("./data/random_new/"));
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

    let mut msgpack_file = File::create("data/out/all_sequences.msgpack")
        .expect("cant create MessagePack export file");

    info!("converting to simplified sequences");
    let simple_sequences: Vec<SimpleSequence> = sequences
        .iter()
        .map(|ddnet_seq| SimpleSequence::from_ddnet_sequence(ddnet_seq))
        .filter(|seq| seq.ticks.len() > 100)
        .collect();

    // MessagePack serialization and timing
    info!("converting to MessagePack");
    let msgpack_start = Instant::now();
    let msgpack_data = to_msgpack(&simple_sequences).unwrap();
    let msgpack_duration = msgpack_start.elapsed();
    info!("MessagePack serialization took {:?}", msgpack_duration);

    info!("writing MessagePack to file");
    msgpack_file.write_all(&msgpack_data).unwrap();
    info!("MessagePack data written to file");

    info!("done :)");
}
