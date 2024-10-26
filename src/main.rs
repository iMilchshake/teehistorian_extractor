use arrow::array::{BooleanArray, Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use clap::Parser;
use log::info;
use log::LevelFilter;
use std::sync::Arc;
use std::{fs::File, path::PathBuf};
use teehistorian_extractor::extractor::{Extractor, SimpleSequence};

fn add_filename_suffix(mut path: PathBuf, suffix: &str) -> PathBuf {
    let stem = path.file_stem().unwrap().to_string_lossy();
    let extension = path.extension().unwrap();
    path.set_file_name(format!(
        "{}_{}.{}",
        stem,
        suffix,
        extension.to_string_lossy()
    ));
    path
}

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

#[derive(Parser, Debug)]
struct Cli {
    /// Input data directory
    #[clap(short, long, default_value = "./data/random/")]
    input: PathBuf,

    /// Output Arrow file path
    #[clap(short, long, default_value = "data/out/sequences.arrow")]
    output: PathBuf,

    /// Minimum ticks per sequence to include
    #[clap(short, long, default_value = "100")]
    min_ticks: usize,

    /// Logging level (error, warn, info, debug, trace)
    #[clap(short, long, default_value = "info")]
    log_level: LevelFilter,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    colog::default_builder()
        .filter_level(args.log_level)
        .target(env_logger::Target::Stdout)
        .init();

    // Parse sequences
    let sequences = Extractor::get_all_ddnet_sequences(args.input);
    info!("extracted {} sequences", sequences.len());

    // Convert to simplified sequences
    let simple_sequences: Vec<SimpleSequence> = sequences
        .iter()
        .map(SimpleSequence::from_ddnet_sequence)
        .filter(|seq| seq.tick_count > args.min_ticks)
        .collect();
    info!("extracted {} sequences", sequences.len());

    // determine total tick count
    let total_ticks = simple_sequences.iter().map(|s| s.tick_count).sum::<usize>();
    info!(
        "total ticks={} equal to {:.1} minutes or {:.1} hours of gameplay",
        total_ticks,
        (total_ticks as f32 / (50. * 60.)),
        (total_ticks as f32 / (50. * 60. * 60.))
    );

    let main_record_batch = to_arrow_flat_ticks(&simple_sequences).unwrap();
    let lookup_record_batch = to_arrow_sequence_info(&simple_sequences).unwrap();

    write_record_batch_to_file(
        &main_record_batch,
        &add_filename_suffix(args.output.clone(), "ticks"),
    )?;
    write_record_batch_to_file(
        &lookup_record_batch,
        &add_filename_suffix(args.output.clone(), "sequences"),
    )?;

    info!("Arrow data written to {:?}", &args.output);
    Ok(())
}
