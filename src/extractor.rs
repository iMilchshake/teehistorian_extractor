use crate::parser::{DDNetSequence, GameInfo, Parser};
use log::info;
use std::{
    fs::{self, File},
    path::PathBuf,
};
use teehistorian::{Th, ThBufReader};

pub struct Extractor {}

impl Extractor {
    pub fn extract(path: PathBuf) -> Vec<DDNetSequence> {
        let mut all_sequences: Vec<DDNetSequence> = Vec::new();

        if path.is_dir() {
            for (file_index, entry) in fs::read_dir(path).unwrap().enumerate() {
                let path = entry.unwrap().path();
                info!(
                    "Parsing index={} name={}",
                    file_index,
                    path.to_string_lossy()
                );
                let sequences = Self::process_file(path);
                all_sequences.extend(sequences);
            }
        } else if path.is_file() {
            info!("Parsing name={}", path.to_string_lossy());
            let sequences = Self::process_file(path);
            all_sequences.extend(sequences);
        }

        all_sequences
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
