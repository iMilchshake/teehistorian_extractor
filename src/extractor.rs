use crate::parser::{DDNetSequence, GameInfo, Parser};
use log::info;
use std::{
    fs::{self, File},
    path::PathBuf,
};
use teehistorian::{Th, ThBufReader};

pub struct SimplifiedTick {
    pos_x: i32,
    pos_y: i32,
    /// left=-1, none=0, right=+1
    move_dir: i32,
    target_x: i32,
    target_y: i32,
    jump: bool,
    fire: bool,
    hook: bool,
    // TODO: weapon? data from playerflags?
}

/// Simplified and more human-readible representation of DDNetSequences.
pub struct SimpleSequence {
    /// the index of the sequences first tick for the corresponding teehistorian file
    start_tick: usize,

    /// all relevant per-tick data
    ticks: Vec<SimplifiedTick>,

    /// name of the corresponding map
    map_name: String,
}

impl SimpleSequence {
    pub fn from_ddnet_sequence(ddnet_sequence: &DDNetSequence) {
        todo!();
    }
}

pub struct Extractor;
impl Extractor {
    pub fn get_sequences(path: PathBuf) -> Vec<DDNetSequence> {
        let mut sequences: Vec<DDNetSequence> = Vec::new();

        if path.is_dir() {
            for (file_index, entry) in fs::read_dir(path).unwrap().enumerate() {
                let path = entry.unwrap().path();
                info!(
                    "Parsing index={} name={}",
                    file_index,
                    path.to_string_lossy()
                );
                sequences.extend(Self::process_file(path));
            }
        } else if path.is_file() {
            info!("Parsing name={}", path.to_string_lossy());
            sequences.extend(Self::process_file(path));
        }

        sequences
    }

    fn process_file(path: PathBuf) -> Vec<DDNetSequence> {
        let mut all_sequences: Vec<DDNetSequence> = Vec::new();

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

        all_sequences.append(&mut parser.completed_sequences);
        all_sequences
    }
}
