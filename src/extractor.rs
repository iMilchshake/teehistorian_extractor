use crate::parser::{DDNetSequence, Parser};
use log::info;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use serde::Serialize;
use std::{
    fs::{self, File},
    path::PathBuf,
};
use teehistorian::{Th, ThBufReader};

// TODO: current weapon -> hard as i need to simulate the entire game state ..
// TODO: player flags?
#[pyclass]
#[derive(Serialize, Debug, Clone)]
pub struct SimplifiedTick {
    #[pyo3(get)]
    pos_x: i32,
    #[pyo3(get)]
    pos_y: i32,
    /// left=-1, none=0, right=+1
    #[pyo3(get)]
    move_dir: i32,
    #[pyo3(get)]
    target_x: i32,
    #[pyo3(get)]
    target_y: i32,
    #[pyo3(get)]
    jump: bool,
    #[pyo3(get)]
    fire: bool,
    #[pyo3(get)]
    hook: bool,
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
#[pyclass]
#[derive(Serialize, Debug)]
pub struct SimpleSequence {
    /// the index of the sequences first tick for the corresponding teehistorian file
    #[pyo3(get)]
    start_tick: usize,

    /// all relevant per-tick data
    #[pyo3(get)]
    ticks: Vec<SimplifiedTick>,

    /// name of player
    #[pyo3(get)]
    player_name: String,
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
        }
    }
}

pub struct Extractor;
impl Extractor {
    pub fn get_all_ddnet_sequences(path: PathBuf) -> Vec<DDNetSequence> {
        let mut sequences: Vec<DDNetSequence> = Vec::new();

        if path.is_dir() {
            for (file_index, entry) in fs::read_dir(path).unwrap().enumerate() {
                let path = entry.unwrap().path();
                info!(
                    "Parsing index={} name={}",
                    file_index,
                    path.to_string_lossy()
                );
                sequences.extend(Self::get_ddnet_sequence(path));
            }
        } else if path.is_file() {
            info!("Parsing name={}", path.to_string_lossy());
            sequences.extend(Self::get_ddnet_sequence(path));
        }

        sequences
    }

    fn get_ddnet_sequence(path: PathBuf) -> Vec<DDNetSequence> {
        let mut all_sequences: Vec<DDNetSequence> = Vec::new();

        let f = File::open(&path).unwrap();
        let mut th = Th::parse(ThBufReader::new(f)).unwrap();

        // TODO: do i need this info?
        // let game_info = GameInfo::from_header_bytes(th.header().unwrap());

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

#[pyfunction]
fn get_simplified_ticks(path: PathBuf) -> PyResult<Vec<SimpleSequence>> {
    let sequences = Extractor::get_all_ddnet_sequences(path);

    let simple_sequences: Vec<SimpleSequence> = sequences
        .iter()
        .map(|ddnet_seq| SimpleSequence::from_ddnet_sequence(ddnet_seq))
        .collect();

    Ok(simple_sequences)
}

#[pymodule]
fn teehistorian_extractor(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add your classes to the module
    m.add_class::<SimplifiedTick>()?;
    m.add_class::<SimpleSequence>()?;

    // Add your functions to the module
    m.add_function(wrap_pyfunction!(get_simplified_ticks, m)?)?;

    // Any additional initialization can be done here

    Ok(())
}
