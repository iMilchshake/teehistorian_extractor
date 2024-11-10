use crate::extractor::Sequence;
use arrow::array::{BooleanArray, Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use ndarray_npy::NpzWriter;
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

/// Initialze empty dataset, use add function to add (batches) of data to it
pub fn initialize_dataset(folder_path: &PathBuf) {
    create_dir_all(folder_path).expect("Failed to create dataset directory");
    File::create(folder_path.join("ticks.npz")).expect("Failed to create ticks.npz");
    let mut csv_file =
        File::create(folder_path.join("sequences.csv")).expect("Failed to create sequences.csv");
    writeln!(csv_file, "player,start,ticks,map").expect("Failed to write header to sequences.csv");
}

/// Store the tick data of sequences in numpy npy files in a npz archive
/// Also store meta data about these sequences in a .json file
/// Initialize a dataset before using this function to add data to it!
pub fn add_to_dataset(sequences: &[Sequence], folder_path: PathBuf) {
    let tick_file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(folder_path.join("ticks.npz"))
        .expect("Failed to open ticks.npz for appending");
    let mut npz_write = NpzWriter::new(tick_file);

    let mut csv_file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(folder_path.join("sequences.csv"))
        .expect("Failed to open sequences.csv for appending");

    for (i, seq) in sequences.iter().enumerate() {
        npz_write
            .add_array(i.to_string(), &seq.ticks_to_array2())
            .expect("Failed to add array to .npz file");
        let meta_csv = format!(
            "{},{},{},{}",
            seq.player_name, seq.start_tick, seq.tick_count, seq.map_name
        );
        writeln!(csv_file, "{}", meta_csv).expect("Failed to write to sequences.csv");
    }

    // Finalize the .npz file
    npz_write.finish().expect("Failed to finalize .npz file");
}
