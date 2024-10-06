use log::{debug, error, info, trace, warn};
use std::fs::{self, File};
use teehistorian::{Th, ThBufReader};

use teehistorian_extractor::parser::{DDNetSequence, GameInfo, Parser};

fn main() {
    colog::default_builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let mut all_sequences: Vec<DDNetSequence> = Vec::new();

    for (file_index, entry) in fs::read_dir("data/random").unwrap().enumerate() {
        let path = entry.unwrap().path();
        info!("{} parsing {}", file_index, path.to_string_lossy());
        let f = File::open(&path).unwrap();
        let mut th = Th::parse(ThBufReader::new(f)).unwrap();

        let game_info = GameInfo::from_header_bytes(th.header().unwrap());
        info!("{:?}", game_info);

        let mut parser = Parser::new();
        while !parser.finished {
            if let Ok(chunk) = th.next_chunk() {
                parser.parse_chunk(chunk);
            } else {
                break;
            }
        }

        info!(
            "parsed {} chunks including {} ticks",
            parser.chunk_index, parser.tick_index
        );

        all_sequences.append(&mut parser.completed_sequences);

        if file_index > 2 {
            break;
        }
    }

    info!("extracted {} sequences", all_sequences.len());

    for (seq_index, sequence) in all_sequences.iter().enumerate() {
        info!("{:?}", &sequence);
        let file = File::create(format!("data/out/output_{}.json", seq_index)).unwrap();
        serde_json::to_writer(file, &sequence).unwrap();
    }

    let total_ticks = all_sequences
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
