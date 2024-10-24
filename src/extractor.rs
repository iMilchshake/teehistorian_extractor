use crate::parser::{DDNetSequence, Parser};
use log::{debug, error, warn};
use serde::Serialize;
use std::{
    fs::{self, File},
    path::PathBuf,
};
use teehistorian::{Th, ThBufReader};

// TODO: current weapon -> hard as i need to simulate the entire game state ..
// TODO: player flags?
#[derive(Serialize, Debug)]
pub struct SimplifiedTick {
    pub pos_x: i32,
    pub pos_y: i32,
    /// left=-1, none=0, right=+1
    pub move_dir: i32,
    pub target_x: i32,
    pub target_y: i32,
    pub jump: bool,
    pub fire: bool,
    pub hook: bool,
}

///     direction: input[0],
///     target_x: input[1],
///     target_y: input[2],
///     jump: input[3],
///     fire: input[4],
///     hook: input[5],
///     player_flags: input[6], // range 0 - 256
///     wanted_weapon: input[7],
///     next_weapon: input[8],
///     prev_weapon: input[9],
impl SimplifiedTick {
    pub fn from_ddnet(input_vector: &[i32; 10], player_position: &(i32, i32)) -> SimplifiedTick {
        SimplifiedTick {
            pos_x: player_position.0,
            pos_y: player_position.1,
            move_dir: input_vector[0],
            target_x: input_vector[1],
            target_y: input_vector[2],
            jump: input_vector[3] == 1,
            fire: (input_vector[4] % 2) == 1, // odd = holding LMB
            hook: input_vector[5] == 1,
        }
    }
}

/// Simplified and more human-readible representation of DDNetSequences.
#[derive(Serialize, Debug)]
pub struct SimpleSequence {
    /// the index of the sequences first tick for the corresponding teehistorian file
    pub start_tick: usize,

    /// all relevant per-tick data
    pub ticks: Vec<SimplifiedTick>,

    /// name of player
    pub player_name: String,

    /// name of map
    pub map_name: String,
}

impl SimpleSequence {
    pub fn from_ddnet_sequence(ddnet_sequence: &DDNetSequence) -> SimpleSequence {
        let start_tick = ddnet_sequence.start_tick as usize;
        let end_tick = ddnet_sequence
            .end_tick
            .expect("ddnet sequence has no end tick") as usize;
        let tick_count = end_tick - start_tick;

        // sanity checks
        assert!(tick_count == ddnet_sequence.input_vectors.len());
        assert!(tick_count == ddnet_sequence.player_positions.len());
        assert!(ddnet_sequence.player_name.is_some());

        // convert data to vec of simplified ticks
        let ticks: Vec<SimplifiedTick> = ddnet_sequence
            .player_positions
            .iter()
            .zip(ddnet_sequence.input_vectors.iter())
            .map(|(player_position, input_vector)| {
                SimplifiedTick::from_ddnet(input_vector, player_position)
            })
            .collect();

        SimpleSequence {
            start_tick,
            ticks,
            player_name: ddnet_sequence.player_name.clone().unwrap(),
            map_name: ddnet_sequence.map_name.clone().unwrap(),
        }
    }
}

pub struct Extractor;
impl Extractor {
    /// Extract all sequences of all teehistorian files in the provided path.
    /// Can either be a folder or an individual teehistorian file.
    pub fn get_all_ddnet_sequences(path: PathBuf) -> Vec<DDNetSequence> {
        let mut sequences: Vec<DDNetSequence> = Vec::new();

        if path.is_dir() {
            for (file_index, entry) in fs::read_dir(path).unwrap().enumerate() {
                let path = entry.unwrap().path();
                debug!(
                    "Parsing index={} name={}",
                    file_index,
                    path.to_string_lossy()
                );
                sequences.extend(Self::get_ddnet_sequences(path));
            }
        } else if path.is_file() {
            debug!("Parsing name={}", path.to_string_lossy());
            sequences.extend(Self::get_ddnet_sequences(path));
        }

        sequences
    }

    /// Extract ddnet sequences for a single teehistorian file
    fn get_ddnet_sequences(path: PathBuf) -> Vec<DDNetSequence> {
        let f = File::open(&path).unwrap();
        let mut th = Th::parse(ThBufReader::new(f)).unwrap();

        let header_bytes = th.header();

        if header_bytes.is_err() {
            error!("coulnt parse header of file {:?}", path);
            return Vec::new();
        }

        let mut parser = Parser::new();
        parser.parse_header(header_bytes.unwrap());
        while let Ok(chunk) = th.next_chunk() {
            let parse_status = parser.parse_chunk(chunk);

            if let Err(err) = parse_status {
                warn!(
                    "path={:?}\nerror={:}\nrecovering {:} completed sequences.",
                    path,
                    err,
                    parser.completed_sequences.len()
                );
                break;
            }
        }

        parser.completed_sequences
    }
}
