use crate::extractor::Sequence;

use hdf5_metno as hdf5;
use ndarray::{Array2, Array3};
use std::{
    collections::HashMap,
    fs::{create_dir_all, File, OpenOptions},
    io::Write,
    path::PathBuf,
};

/// keeps track of relevant meta-data to remain consistent even among batched export
pub struct Exporter {
    /// player_name -> (player_id, sequence_count)
    pub players: HashMap<String, (usize, usize)>,

    /// amount of registered players
    pub player_count: usize,

    /// total amount of sequences
    pub sequence_count: usize,

    seq_length: usize,
    num_features: usize,
    seq_dataset: hdf5::Dataset,
    meta_file: File,
}

impl Exporter {
    /// Initialze empty dataset, use add function to add (batches) of data to it
    pub fn new(folder_path: &PathBuf, seq_length: usize, num_features: usize) -> Exporter {
        create_dir_all(folder_path).expect("Failed to create dataset directory");

        // initialize sequences hdf5 file
        let seq_file = hdf5::File::create(folder_path.join("sequences.h5"))
            .expect("Failed to create sequences.h5");
        let seq_dataset = seq_file
            .new_dataset::<i32>()
            .shape((hdf5::Extent::resizable(0), seq_length, num_features))
            .create("sequences")
            .expect("failed to create sequences.h5");

        // initialize meta
        let mut meta_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(folder_path.join("meta.csv"))
            .unwrap();
        writeln!(meta_file, "seq_id,player_id,player,start,ticks,map")
            .expect("Failed to write header to meta.csv");

        Exporter {
            players: HashMap::new(),
            player_count: 0,
            sequence_count: 0,
            seq_dataset,
            meta_file,
            num_features,
            seq_length,
        }
    }

    /// Store the tick data of sequences in numpy npy files in a npz archive
    /// Also store meta data about these sequences in a .json file
    /// Initialize a dataset before using this function to add data to it!
    pub fn add_to_dataset(&mut self, sequences: &[Sequence]) {
        let mut tick_data =
            Array3::<i32>::zeros((sequences.len(), self.seq_length, self.num_features));
        for (seq_index, seq) in sequences.iter().enumerate() {
            // add new entry if player name is seen for first time
            if !self.players.contains_key(&seq.player_name) {
                // use current player count as id for player
                self.players
                    .insert(seq.player_name.clone(), (self.player_count, 0));
                self.player_count += 1;
            }
            let player = self.players.get_mut(&seq.player_name).unwrap();

            // increment seq count for player
            player.1 += 1;

            // add array2 representation of sequence
            let sequence_ticks: Array2<i32> = seq.ticks_to_feature_array();
            tick_data
                .index_axis_mut(ndarray::Axis(0), seq_index)
                .assign(&sequence_ticks);

            let meta_csv = format!(
                "{},{},\"{}\",{},{},{}",
                self.sequence_count,
                player.0,                          // player_id
                seq.player_name.replace("\"", ""), // get rid of quotes for easer parsing
                seq.start_tick,
                seq.tick_count,
                seq.map_name
            );
            writeln!(self.meta_file, "{}", meta_csv).expect("Failed to write to sequences.csv");

            self.sequence_count += 1;
        }

        // Append ALL sequence ticks to seq_dataset
        let current_size = self.seq_dataset.shape()[0];
        let new_size = current_size + tick_data.shape()[0];
        dbg!(current_size, new_size);
        self.seq_dataset
            .resize((new_size, self.seq_length, self.num_features))
            .expect("Failed to resize dataset");
        self.seq_dataset
            .write_slice(&tick_data.view(), (current_size..new_size, .., ..))
            .expect("Failed to write data");
    }
}
