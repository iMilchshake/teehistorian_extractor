use crate::extractor::SimpleSequence;
use arrow::array::{BooleanArray, Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use ndarray::Array2;
use ndarray_npy::NpzWriter;
use std::sync::Arc;
use std::{fs::File, path::PathBuf};

fn int32_array<F>(sequences: &[SimpleSequence], f: F) -> Arc<dyn arrow::array::Array>
where
    F: Fn(&SimpleSequence) -> Vec<i32>,
{
    Arc::new(Int32Array::from(
        sequences.iter().flat_map(|s| f(s)).collect::<Vec<i32>>(),
    ))
}

fn bool_array<F>(sequences: &[SimpleSequence], f: F) -> Arc<dyn arrow::array::Array>
where
    F: Fn(&SimpleSequence) -> Vec<bool>,
{
    Arc::new(BooleanArray::from(
        sequences.iter().flat_map(|s| f(s)).collect::<Vec<bool>>(),
    ))
}

fn to_arrow_flat_ticks(sequences: &[SimpleSequence]) -> Option<RecordBatch> {
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

fn to_arrow_sequence_info(sequences: &[SimpleSequence]) -> Option<RecordBatch> {
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

pub fn export_to_dir(sequences: &[SimpleSequence], output_path: &PathBuf) {
    assert!(output_path.is_dir());

    let main_record_batch = to_arrow_flat_ticks(&sequences).unwrap();
    let lookup_record_batch = to_arrow_sequence_info(&sequences).unwrap();

    let ticks_path = output_path.join("ticks.arrow");
    let sequences_path = output_path.join("sequences.arrow");

    write_record_batch_to_file(&main_record_batch, &ticks_path).unwrap();
    write_record_batch_to_file(&lookup_record_batch, &sequences_path).unwrap();
}

pub fn convert_and_save_sequences_to_npz(sequences: &[SimpleSequence], file_path: &str) {
    let num_fields = 8;

    // Create a new .npz file
    let file = File::create(file_path).expect("Failed to create .npz file");
    let mut npz = NpzWriter::new(file);

    for (i, seq) in sequences.iter().enumerate() {
        let sequence_len = seq.pos_x.len();

        // Pre-allocate a single Vec with enough space for the current sequence
        let mut data = Vec::with_capacity(sequence_len * num_fields);

        data.extend_from_slice(&seq.pos_x);
        data.extend_from_slice(&seq.pos_y);
        data.extend_from_slice(&seq.move_dir);
        data.extend_from_slice(&seq.target_x);
        data.extend_from_slice(&seq.target_y);

        // Convert bool Vecs to i32 on the fly
        data.extend(seq.jump.iter().map(|&b| b as i32));
        data.extend(seq.fire.iter().map(|&b| b as i32));
        data.extend(seq.hook.iter().map(|&b| b as i32));

        // Convert the flat Vec to an Array2 where each row is a timestep across fields
        let array = Array2::from_shape_vec((sequence_len, num_fields), data)
            .expect("Shape should match data length");

        // Save the array to the .npz file with a unique name
        npz.add_array(i.to_string(), &array)
            .expect("Failed to add array to .npz file");
    }

    // Finalize the .npz file
    npz.finish().expect("Failed to finalize .npz file");
}
