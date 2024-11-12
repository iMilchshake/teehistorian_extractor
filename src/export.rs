use crate::extractor::Sequence;
use arrow::array::{BooleanArray, Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use ndarray_npy::NpzWriter;
use std::collections::HashMap;
use std::sync::Arc;
use std::{fs::File, path::PathBuf};

use std::fs::create_dir_all;
use std::fs::OpenOptions;
use std::io::Write;

fn int32_array<F>(sequences: &[Sequence], f: F) -> Arc<dyn arrow::array::Array>
where
    F: Fn(&Sequence) -> Vec<i32>,
{
    Arc::new(Int32Array::from(
        sequences.iter().flat_map(|s| f(s)).collect::<Vec<i32>>(),
    ))
}

fn bool_array<F>(sequences: &[Sequence], f: F) -> Arc<dyn arrow::array::Array>
where
    F: Fn(&Sequence) -> Vec<bool>,
{
    Arc::new(BooleanArray::from(
        sequences.iter().flat_map(|s| f(s)).collect::<Vec<bool>>(),
    ))
}

fn to_arrow_flat_ticks(sequences: &[Sequence]) -> Option<RecordBatch> {
    let sequence_ids: Vec<i32> = sequences
        .iter()
        .enumerate()
        .flat_map(|(seq_id, seq)| vec![seq_id as i32; seq.pos_x.len()])
        .collect();

    let schema = Schema::new(vec![
        Field::new("sequence_id", DataType::Int32, false),
        Field::new("pos_x", DataType::Int32, false),
        Field::new("pos_y", DataType::Int32, false),
        Field::new("move_dir", DataType::Int32, false),
        Field::new("target_x", DataType::Int32, false),
        Field::new("target_y", DataType::Int32, false),
        Field::new("jump", DataType::Boolean, false),
        Field::new("fire", DataType::Boolean, false),
        Field::new("hook", DataType::Boolean, false),
    ]);

    let arrays = vec![
        Arc::new(Int32Array::from(sequence_ids)),
        int32_array(sequences, |s| s.pos_x.clone()),
        int32_array(sequences, |s| s.pos_y.clone()),
        int32_array(sequences, |s| s.move_dir.clone()),
        int32_array(sequences, |s| s.target_x.clone()),
        int32_array(sequences, |s| s.target_y.clone()),
        bool_array(sequences, |s| s.jump.clone()),
        bool_array(sequences, |s| s.fire.clone()),
        bool_array(sequences, |s| s.hook.clone()),
    ];

    RecordBatch::try_new(Arc::new(schema), arrays).ok()
}

fn to_arrow_sequence_info(sequences: &[Sequence]) -> Option<RecordBatch> {
    let schema = Schema::new(vec![
        Field::new("sequence_id", DataType::Int32, false),
        Field::new("start_tick", DataType::UInt64, false),
        Field::new("tick_count", DataType::UInt64, false),
        Field::new("player_name", DataType::Utf8, false),
        Field::new("map_name", DataType::Utf8, false),
    ]);

    let sequence_ids: Vec<i32> = (0..sequences.len() as i32).collect();
    let start_ticks: Vec<u64> = sequences.iter().map(|s| s.start_tick as u64).collect();
    let tick_counts: Vec<u64> = sequences.iter().map(|s| s.tick_count as u64).collect();
    let player_names: Vec<String> = sequences.iter().map(|s| s.player_name.clone()).collect();
    let map_names: Vec<String> = sequences.iter().map(|s| s.map_name.clone()).collect();

    let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(Int32Array::from(sequence_ids)),
        Arc::new(UInt64Array::from(start_ticks)),
        Arc::new(UInt64Array::from(tick_counts)),
        Arc::new(StringArray::from(player_names)),
        Arc::new(StringArray::from(map_names)),
    ];

    RecordBatch::try_new(Arc::new(schema), arrays).ok()
}

fn write_record_batch_to_file(
    record_batch: &RecordBatch,
    output_path: &PathBuf,
) -> Result<(), ArrowError> {
    let file = File::create(output_path)?;
    let mut writer = FileWriter::try_new(file, &record_batch.schema())?;
    writer.write(record_batch)?;
    writer.finish()?;
    Ok(())
}

pub fn export_to_dir(sequences: &[Sequence], output_path: &PathBuf) {
    assert!(output_path.is_dir());

    let main_record_batch = to_arrow_flat_ticks(&sequences).unwrap();
    let lookup_record_batch = to_arrow_sequence_info(&sequences).unwrap();

    let ticks_path = output_path.join("ticks.arrow");
    let sequences_path = output_path.join("sequences.arrow");

    write_record_batch_to_file(&main_record_batch, &ticks_path).unwrap();
    write_record_batch_to_file(&lookup_record_batch, &sequences_path).unwrap();
}

/// keeps track of relevant meta-data to remain consistent even among batched export
pub struct Exporter {
    /// player_name -> (player_id, sequence_count)
    pub players: HashMap<String, (usize, usize)>,

    /// amount of registered players
    pub player_count: usize,

    /// total amount of sequences
    pub sequence_count: usize,

    /// path to target dataset folder
    pub folder_path: PathBuf,

    /// keeps writer to npz archive open
    npz_writer: NpzWriter<File>,
}

impl Exporter {
    /// initializes empty dataset and Exporter
    pub fn new(folder_path: &PathBuf) -> Exporter {
        let npz_writer = Exporter::initialize_dataset(folder_path);
        Exporter {
            players: HashMap::new(),
            player_count: 0,
            sequence_count: 0,
            folder_path: folder_path.clone(),
            npz_writer,
        }
    }

    /// Initialze empty dataset, use add function to add (batches) of data to it
    /// Returns NpzWriter used to write tick data to npz archive
    fn initialize_dataset(folder_path: &PathBuf) -> NpzWriter<File> {
        create_dir_all(folder_path).expect("Failed to create dataset directory");
        File::create(folder_path.join("ticks.npz")).expect("Failed to create ticks.npz");
        let mut csv_file = File::create(folder_path.join("sequences.csv"))
            .expect("Failed to create sequences.csv");
        writeln!(csv_file, "seq_id,player_id,player,start,ticks,map")
            .expect("Failed to write header to sequences.csv");

        let tick_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(folder_path.join("ticks.npz"))
            .expect("Failed to create ticks.npz");

        NpzWriter::new(tick_file)
    }

    /// Store the tick data of sequences in numpy npy files in a npz archive
    /// Also store meta data about these sequences in a .json file
    /// Initialize a dataset before using this function to add data to it!
    pub fn add_to_dataset(&mut self, sequences: &[Sequence]) {
        let mut csv_file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.folder_path.join("sequences.csv"))
            .expect("Failed to open sequences.csv for appending");

        for (_, seq) in sequences.iter().enumerate() {
            // add new entry if player name is seen for first time
            if !self.players.contains_key(&seq.player_name) {
                // use current player count as id for player
                self.players
                    .insert(seq.player_name.clone(), (self.player_count, 1));
                self.player_count += 1;
            }
            let player = self.players.get_mut(&seq.player_name).unwrap();

            // increment seq count for player
            player.1 += 1;

            self.npz_writer
                .add_array(self.sequence_count.to_string(), &seq.ticks_to_array2())
                .expect("Failed to add array to .npz file");
            let meta_csv = format!(
                "{},{},{},{},{},{}",
                self.sequence_count,
                player.0, // player_id
                seq.player_name,
                seq.start_tick,
                seq.tick_count,
                seq.map_name
            );
            writeln!(csv_file, "{}", meta_csv).expect("Failed to write to sequences.csv");

            self.sequence_count += 1;
        }
    }

    pub fn finalize(self) {
        self.npz_writer
            .finish()
            .expect("Failed to finalize .npz file");
    }
}
