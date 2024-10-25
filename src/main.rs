use arrow::array::{BooleanArray, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use clap::Parser;
use env_logger::{Builder, Target};
use log::info;
use log::LevelFilter;
use std::sync::Arc;
use std::{fs::File, path::PathBuf};
use teehistorian_extractor::extractor::{Extractor, SimpleSequence};

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
        .filter(|seq| seq.ticks.len() > args.min_ticks)
        .collect();

    info!("extracted {} sequences", sequences.len());

    let total_ticks = simple_sequences
        .iter()
        .map(|s| s.ticks.len())
        .sum::<usize>();

    info!(
        "total ticks={} equal to {:.1} minutes or {:.1} hours of gameplay",
        total_ticks,
        (total_ticks as f32 / (50. * 60.)),
        (total_ticks as f32 / (50. * 60. * 60.))
    );

    // Flatten data across all sequences
    let mut sequence_ids = Vec::new();
    let mut pos_x = Vec::new();
    let mut pos_y = Vec::new();
    let mut move_dir = Vec::new();
    let mut target_x = Vec::new();
    let mut target_y = Vec::new();
    let mut jump = Vec::new();
    let mut fire = Vec::new();
    let mut hook = Vec::new();

    for (seq_id, sequence) in simple_sequences.iter().enumerate() {
        for tick in &sequence.ticks {
            sequence_ids.push(seq_id as i32);
            pos_x.push(tick.pos_x);
            pos_y.push(tick.pos_y);
            move_dir.push(tick.move_dir);
            target_x.push(tick.target_x);
            target_y.push(tick.target_y);
            jump.push(tick.jump);
            fire.push(tick.fire);
            hook.push(tick.hook);
        }
    }

    info!("flattened");

    // Define the schema
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

    // Create Arrow arrays
    let sequence_id_array = Int32Array::from(sequence_ids);
    let pos_x_array = Int32Array::from(pos_x);
    let pos_y_array = Int32Array::from(pos_y);
    let move_dir_array = Int32Array::from(move_dir);
    let target_x_array = Int32Array::from(target_x);
    let target_y_array = Int32Array::from(target_y);
    let jump_array = BooleanArray::from(jump);
    let fire_array = BooleanArray::from(fire);
    let hook_array = BooleanArray::from(hook);

    info!("to arrow");

    // Create a RecordBatch
    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(sequence_id_array),
            Arc::new(pos_x_array),
            Arc::new(pos_y_array),
            Arc::new(move_dir_array),
            Arc::new(target_x_array),
            Arc::new(target_y_array),
            Arc::new(jump_array),
            Arc::new(fire_array),
            Arc::new(hook_array),
        ],
    )?;

    // Write to an Arrow file
    let file = File::create(&args.output)?;
    let mut writer = FileWriter::try_new(file, &record_batch.schema())?;
    writer.write(&record_batch)?;
    writer.finish()?;

    info!("Arrow data written to {:?}", &args.output);
    Ok(())
}
