use log::{error, warn};
use std::collections::HashMap;
use teehistorian::chunks::{InputDiff, InputNew, PlayerDiff, PlayerNew};

/// A tick defines the input vectors and player positions for a timestep.
/// With the exception of the first tick, the previous tick is copied during
/// parsing and only the changes are applied. This means that after successful parsing,
/// all implicit information is explicitly available for each tick.
#[derive(Clone, Debug)]
pub struct Tick {
    /// tracks input vectors for each cid
    pub input_vectors: HashMap<i32, [i32; 10]>,

    /// tracks player position for each cid (x, y)
    pub player_positions: HashMap<i32, (i32, i32)>,
}

impl Tick {
    /// initializes an empty tick struct
    pub fn new() -> Tick {
        Tick {
            input_vectors: HashMap::new(),
            player_positions: HashMap::new(),
        }
    }

    /// Add inital player position based on PlayerNew chunk
    pub fn add_init_position(&mut self, new_player: PlayerNew) {
        assert!(!self.player_positions.contains_key(&new_player.cid));
        self.player_positions
            .insert(new_player.cid, (new_player.x, new_player.y));
    }

    /// Add initial player input based on PlayerNew chunk
    pub fn add_init_input(&mut self, input_new: InputNew) {
        if self.input_vectors.contains_key(&input_new.cid) {
            warn!(
                "OVERWRITE: for cid={} an input vector exists={:?}, new input vector={:?}",
                &input_new.cid,
                self.input_vectors.get(&input_new.cid).unwrap(),
                input_new.input
            );
            panic!();
        }
        self.input_vectors.insert(input_new.cid, input_new.input);
    }

    /// Update tick's input vector for some cid given a InputDiff
    pub fn apply_input_diff(&mut self, input_diff: InputDiff) {
        if !self.input_vectors.contains_key(&input_diff.cid) {
            error!(
                "expected input vector for cid={} -> {:?}",
                &input_diff.cid, &input_diff.dinput
            );
            return;
        }

        let input = self
            .input_vectors
            .get_mut(&input_diff.cid)
            .expect("no input vector for cid exists yet");

        // apply input diff to current input
        for i in 0..10 {
            input[i] = input[i].wrapping_add(input_diff.dinput[i]); // TODO: why wrapping?
        }
    }

    /// Update tick's position for some cid given a PlayerDiff
    pub fn apply_position_diff(&mut self, player_diff: PlayerDiff) {
        let position = self
            .player_positions
            .get_mut(&player_diff.cid)
            .expect("no position for cid exists yet");

        position.0 += player_diff.dx;
        position.1 += player_diff.dy;
    }

    /// remove player position for PlayerOld events
    pub fn remove_player_position(&mut self, cid: i32) {
        self.player_positions
            .remove(&cid)
            .expect("no position for cid exists");
    }
}
