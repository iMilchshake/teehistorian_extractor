use crate::extractor::Sequence;

use hdf5_metno::{self as hdf5, types::VarLenAscii};
use ndarray::{Array2, Array3};
use std::{
    collections::HashMap,
    fs::{create_dir_all, File, OpenOptions},
    io::Write,
    path::PathBuf,
};

const MAX_AIM_DISTANCE: f32 = 1000.0;

fn bool_to_unit_f32(b: bool) -> f32 {
    if b {
        1.0
    } else {
        0.0
    }
}

pub struct ExportConfig {
    seq_length: usize,
    use_vel: bool,
    use_rel_target: bool,
    use_aim_angle: bool,
    use_aim_distance: bool,
}

impl ExportConfig {
    pub fn new(
        input_seq_length: usize,
        use_vel: bool,
        use_rel_target: bool,
        use_aim_angle: bool,
        use_aim_distance: bool,
    ) -> ExportConfig {
        ExportConfig {
            seq_length: input_seq_length,
            use_vel,
            use_rel_target,
            use_aim_angle,
            use_aim_distance,
        }
    }
}

/// keeps track of relevant meta-data to remain consistent even among batched export
pub struct Exporter {
    /// player_name -> (player_id, sequence_count)
    pub players: HashMap<String, (usize, usize)>,

    /// amount of registered players
    pub player_count: usize,

    /// total amount of sequences
    pub sequence_count: usize,

    num_features: usize,

    seq_dataset: hdf5::Dataset,
    meta_file: File,

    config: ExportConfig,
}

impl Exporter {
    /// Initialze empty dataset, use add function to add (batches) of data to it
    pub fn new(folder_path: &PathBuf, config: ExportConfig) -> Exporter {
        create_dir_all(folder_path).expect("Failed to create dataset directory");

        let column_names = Exporter::get_column_names(
            config.use_vel,
            config.use_rel_target,
            config.use_aim_angle,
            config.use_aim_distance,
        );
        let num_features = column_names.len();

        // if we use velocity, we need to cut off the last tick as velocity cant be calculated
        let mut config = config;
        if config.use_vel {
            config.seq_length -= 1;
        }

        // initialize sequences hdf5 file
        let seq_file = hdf5::File::create(folder_path.join("sequences.h5"))
            .expect("Failed to create sequences.h5");
        let seq_dataset = seq_file
            .new_dataset::<f32>()
            .shape((hdf5::Extent::resizable(0), config.seq_length, num_features))
            .create("sequences")
            .expect("failed to create sequences.h5");

        // add column named header attribute
        let column_names_vla: Vec<VarLenAscii> = column_names
            .iter()
            .map(|s| VarLenAscii::from_ascii(s.as_bytes()).unwrap())
            .collect();
        let attr = seq_dataset
            .new_attr::<VarLenAscii>()
            .shape(column_names_vla.len())
            .create("column_names")
            .expect("Failed to create column_names attribute");
        attr.write(&column_names_vla)
            .expect("Failed to write column_names attribute");

        // initialize meta
        let mut meta_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(folder_path.join("meta.csv"))
            .unwrap();
        writeln!(meta_file, "seq_id,player_id,player,start,ticks,map,teehist")
            .expect("Failed to write header to meta.csv");

        Exporter {
            players: HashMap::new(),
            player_count: 0,
            sequence_count: 0,
            seq_dataset,
            meta_file,
            num_features,
            config,
        }
    }

    fn get_column_names(
        use_vel: bool,
        use_rel_target: bool,
        use_aim_angle: bool,
        use_aim_distance: bool,
    ) -> Vec<String> {
        let mut column_names = vec![
            "move_dir".to_string(),
            "jump".to_string(),
            "fire".to_string(),
            "hook".to_string(),
        ];

        if use_vel {
            column_names.push("vel_x".to_string());
            column_names.push("vel_y".to_string());
        }

        if use_rel_target {
            column_names.push("target_rel_x".to_string());
            column_names.push("target_rel_y".to_string());
        }

        if use_aim_angle {
            column_names.push("aim_angle".to_string());
        }

        if use_aim_distance {
            column_names.push("aim_distance".to_string());
        }

        column_names
    }

    fn sequence_to_tick_array(&self, seq: &Sequence) -> Array2<f32> {
        let mut data = Vec::new();
        data.extend(
            seq.move_dir
                .iter()
                .take(self.config.seq_length)
                .map(|&i| i as f32),
        );
        data.extend(
            seq.jump
                .iter()
                .take(self.config.seq_length)
                .map(|&b| bool_to_unit_f32(b)),
        );
        data.extend(
            seq.fire
                .iter()
                .take(self.config.seq_length)
                .map(|&b| bool_to_unit_f32(b)),
        );
        data.extend(
            seq.hook
                .iter()
                .take(self.config.seq_length)
                .map(|&b| bool_to_unit_f32(b)),
        );

        if self.config.use_vel {
            let vel_x: Vec<f32> = seq.pos_x.windows(2).map(|w| (w[1] - w[0]) as f32).collect();
            let vel_y: Vec<f32> = seq.pos_y.windows(2).map(|w| (w[1] - w[0]) as f32).collect();
            data.extend(&vel_x[..self.config.seq_length]);
            data.extend(&vel_y[..self.config.seq_length]);
        }

        if self.config.use_rel_target {
            data.extend(
                seq.target_x
                    .iter()
                    .take(self.config.seq_length)
                    .map(|&i| i as f32),
            );
            data.extend(
                seq.target_y
                    .iter()
                    .take(self.config.seq_length)
                    .map(|&i| i as f32),
            );
        }

        if self.config.use_aim_angle {
            data.extend(
                seq.target_x
                    .iter()
                    .zip(seq.target_y.iter())
                    .take(self.config.seq_length)
                    .map(|(&x, &y)| (y as f32).atan2(x as f32).to_degrees()),
            );
        }

        if self.config.use_aim_distance {
            data.extend(
                seq.target_x
                    .iter()
                    .zip(seq.target_y.iter())
                    .take(self.config.seq_length)
                    .map(|(&x, &y)| ((x.pow(2) + y.pow(2)) as f32).sqrt().min(MAX_AIM_DISTANCE)),
            );
        }

        assert!((data.len() % self.config.seq_length) == 0);
        let n_features = data.len() / self.config.seq_length;

        let data_array = Array2::from_shape_vec((n_features, self.config.seq_length), data)
            .expect("shape mismatch while converting sequence to ndarray")
            .reversed_axes(); // transpose to (seq_length, n_features)

        data_array
    }

    pub fn add_to_dataset(&mut self, sequences: &[Sequence]) {
        let mut tick_data =
            Array3::<f32>::zeros((sequences.len(), self.config.seq_length, self.num_features));
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

            let meta_csv = format!(
                "{},{},\"{}\",{},{},{},{}",
                self.sequence_count,
                player.0,                          // player_id
                seq.player_name.replace("\"", ""), // get rid of quotes for easer parsing
                seq.start_tick,
                seq.tick_count,
                seq.map_name,
                seq.teehist_name
            );
            writeln!(self.meta_file, "{}", meta_csv).expect("Failed to write to sequences.csv");

            // add array2 representation of sequence
            let sequence_ticks = self.sequence_to_tick_array(seq);
            tick_data
                .index_axis_mut(ndarray::Axis(0), seq_index)
                .assign(&sequence_ticks);

            self.sequence_count += 1;
        }

        // Append ALL sequence ticks to seq_dataset
        let current_size = self.seq_dataset.shape()[0];
        let new_size = current_size + tick_data.shape()[0];
        dbg!(current_size, new_size);
        self.seq_dataset
            .resize((new_size, self.config.seq_length, self.num_features))
            .expect("Failed to resize dataset");
        self.seq_dataset
            .write_slice(&tick_data.view(), (current_size..new_size, .., ..))
            .expect("Failed to write data");
    }
}
