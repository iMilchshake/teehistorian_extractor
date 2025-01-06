use crate::parser::{DDNetSequence, Parser, ParserConfig};
use log::{debug, error, warn};
use serde::Serialize;
use std::{
    fs::{self, File},
    path::PathBuf,
};
use teehistorian::{Th, ThBufReader};

/// Simplified and more human-readible representation of DDNetSequences.
#[derive(Serialize, Debug)]
pub struct Sequence {
    // sequence data
    pub start_tick: usize,
    pub tick_count: usize,
    pub player_name: String,
    pub map_name: String,
    pub teehist_name: String,

    // tick data
    pub pos_x: Vec<i32>,
    pub pos_y: Vec<i32>,
    pub move_dir: Vec<i32>,
    pub target_x: Vec<i32>,
    pub target_y: Vec<i32>,
    pub jump: Vec<bool>,
    pub fire: Vec<bool>,
    pub hook: Vec<bool>,
}

impl Sequence {
    pub fn from_ddnet_sequence(ddnet_sequence: &DDNetSequence) -> Sequence {
        let start_tick = ddnet_sequence.start_tick as usize;
        let end_tick = ddnet_sequence
            .end_tick
            .expect("ddnet sequence has no end tick") as usize;
        let tick_count = end_tick - start_tick;

        // Sanity checks
        assert!(tick_count == ddnet_sequence.input_vectors.len());
        assert!(tick_count == ddnet_sequence.player_positions.len());
        assert!(ddnet_sequence.player_name.is_some());
        assert!(ddnet_sequence.teehist_path.is_some());

        // prepare vecs for all tick data
        let mut pos_x = Vec::with_capacity(tick_count);
        let mut pos_y = Vec::with_capacity(tick_count);
        let mut move_dir = Vec::with_capacity(tick_count);
        let mut target_x = Vec::with_capacity(tick_count);
        let mut target_y = Vec::with_capacity(tick_count);
        let mut jump = Vec::with_capacity(tick_count);
        let mut fire = Vec::with_capacity(tick_count);
        let mut hook = Vec::with_capacity(tick_count);

        for (player_position, input_vector) in ddnet_sequence
            .player_positions
            .iter()
            .zip(ddnet_sequence.input_vectors.iter())
        {
            pos_x.push(player_position.0);
            pos_y.push(player_position.1);
            move_dir.push(input_vector[0]);
            target_x.push(input_vector[1]);
            target_y.push(input_vector[2]);
            jump.push(input_vector[3] == 1);
            fire.push((input_vector[4] % 2) == 1); // odd = holding LMB
            hook.push(input_vector[5] == 1);
        }

        Sequence {
            start_tick,
            tick_count,
            pos_x,
            pos_y,
            move_dir,
            target_x,
            target_y,
            jump,
            fire,
            hook,
            player_name: ddnet_sequence.player_name.clone().unwrap(),
            map_name: ddnet_sequence.map_name.clone().unwrap(),
            teehist_name: ddnet_sequence.teehist_path.clone().unwrap(),
        }
    }

    // pub fn meta_to_csv(&self) -> String {
    //     format!(
    //         "{},{},{},{},{}",
    //         self.tick_count, self.player_name, self.start_tick, self.map_name, self.teehist_name
    //     )
    // }
}

pub struct Extractor;
impl Extractor {
    /// Extract all sequences of all teehistorian files in the provided path.
    /// Can either be a folder or an individual teehistorian file.
    pub fn get_all_ddnet_sequences(path: PathBuf, config: &ParserConfig) -> Vec<DDNetSequence> {
        let mut sequences: Vec<DDNetSequence> = Vec::new();

        if path.is_dir() {
            for (file_index, entry) in fs::read_dir(path).unwrap().enumerate() {
                let path = entry.unwrap().path();
                debug!(
                    "Parsing index={} name={}",
                    file_index,
                    path.to_string_lossy()
                );
                sequences.extend(Extractor::get_ddnet_sequences(&path, config));
            }
        } else if path.is_file() {
            debug!("Parsing name={}", path.to_string_lossy());
            sequences.extend(Extractor::get_ddnet_sequences(&path, config));
        }

        sequences
    }

    /// Extract ddnet sequences for a single teehistorian file
    pub fn get_ddnet_sequences(path: &PathBuf, config: &ParserConfig) -> Vec<DDNetSequence> {
        let f = File::open(&path).unwrap();
        let mut th = Th::parse(ThBufReader::new(f)).unwrap();

        let header_bytes = th.header();

        if header_bytes.is_err() {
            error!("coulnt parse header of file {:?}", path);
            return Vec::new();
        }

        let mut parser = Parser::new(config.clone());
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

        // add teehistorian file name to all extracted sequences
        for ddnet_seq in parser.completed_sequences.iter_mut() {
            ddnet_seq.teehist_path = path
                .file_stem()
                .and_then(|s| s.to_str().map(|str_val| str_val.to_string()));
        }

        parser.completed_sequences
    }
}
