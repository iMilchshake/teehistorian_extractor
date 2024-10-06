use core::str;
use derivative::Derivative;
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::collections::HashMap;
use teehistorian::chunks::{
    ConsoleCommand, Drop, InputDiff, InputNew, NetMessage, PlayerDiff, PlayerNew, PlayerOld,
    TickSkip,
};
use teehistorian::Chunk;
use twgame_core::net_msg::{self, Team};

use crate::tick::Tick;

#[derive(Debug, Deserialize)]
pub struct GameInfo {
    server_name: String,
    map_name: String,
}

impl GameInfo {
    pub fn from_header_bytes(header_bytes: &[u8]) -> Self {
        let header_str =
            str::from_utf8(header_bytes).expect("failed to convert header_bytes to utf-8");
        let game_info = from_str(header_str).expect("failed to extract GameInfo from header_str");
        game_info
    }
}

/// Sequence of parsed player inputs and positions.
/// Truthful to original DDNet representations.
///
/// # Documentation for input_vectors
/// https://gitlab.com/ddnet-rs/twgame/-/blob/594f3f4869d34d0382ecceeaeb52cf81853ade7c/twgame-core/src/lib.rs#L93
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
#[derive(Derivative, Serialize)]
#[derivative(Debug)]
pub struct DDNetSequence {
    pub cid: i32,
    pub start_tick: i32,
    pub end_tick: Option<i32>,
    pub player_name: Option<String>,
    #[derivative(Debug = "ignore")]
    pub input_vectors: Vec<[i32; 10]>,
    #[derivative(Debug = "ignore")]
    pub player_positions: Vec<(i32, i32)>,
}

impl DDNetSequence {
    pub fn new(cid: i32, start_tick: i32) -> DDNetSequence {
        DDNetSequence {
            cid,
            start_tick,
            end_tick: None,
            player_name: None,
            input_vectors: Vec::new(),
            player_positions: Vec::new(),
        }
    }
}

/// tracks state while parsing teehistorian file
pub struct Parser {
    /// tracks if end of stream (EOS) chunk has already been parsed
    pub finished: bool,

    /// tracks current tick index
    pub tick_index: i32,

    /// tracks chunk index
    pub chunk_index: u32,

    /// tracks last seen cid in a player event (for implicit ticks)
    last_cid: i32,

    /// tracks current tick
    current_tick: Tick,

    /// tracks all previous ticks
    previous_ticks: Vec<Tick>,

    /// tracks all active sequences
    active_sequences: HashMap<i32, DDNetSequence>,

    /// tracks all completed sequences
    pub completed_sequences: Vec<DDNetSequence>,

    /// tracks player names
    player_names: HashMap<i32, String>,

    map_name: Option<String>,
}

impl Parser {
    pub fn new() -> Parser {
        Parser {
            finished: false,
            tick_index: 0,
            chunk_index: 0,
            last_cid: -1, // can never be larger or equal to first cid
            current_tick: Tick::new(),
            previous_ticks: Vec::new(),
            active_sequences: HashMap::new(),
            completed_sequences: Vec::new(),
            player_names: HashMap::new(),
            map_name: None,
        }
    }

    /// Skips dt+1 ticks. In the case of dt=0 this just "finalizes" the current tick
    fn handle_tick_skip(&mut self, skip: TickSkip) {
        trace!("T={}\t{:?}", self.tick_index, skip);

        self.tick_index += 1 + skip.dt;
        for _ in 0..(skip.dt + 1) {
            self.previous_ticks.push(self.current_tick.clone());
        }
    }

    fn handle_input_new(&mut self, input_new: InputNew) {
        debug!("T={} {:?}", self.tick_index, &input_new);
        self.current_tick.add_init_input(input_new);
    }

    fn handle_input_diff(&mut self, input_diff: InputDiff) {
        trace!("T={} {:?}", self.tick_index, &input_diff);
        self.current_tick.apply_input_diff(input_diff);
    }

    fn handle_net_message(&mut self, net_msg: NetMessage) {
        let res = net_msg::parse_net_msg(&net_msg.msg, &mut net_msg::NetVersion::V06);
        if let Ok(res) = res {
            match res {
                net_msg::ClNetMessage::ClStartInfo(info) => {
                    let player_name = String::from_utf8_lossy(info.name).to_string();
                    debug!("StartInfo cid={} => name={}", net_msg.cid, player_name);
                    self.player_names.insert(net_msg.cid, player_name);
                }
                net_msg::ClNetMessage::ClKill => {
                    debug!("tick={} cid={} KILL", self.tick_index, net_msg.cid);
                }
                net_msg::ClNetMessage::ClSetTeam(team) => match team {
                    Team::Spectators => {
                        debug!("cid={} to spec", net_msg.cid);
                    }
                    Team::Red | Team::Blue => {
                        debug!("cid={} to red/blue", net_msg.cid);
                    }
                },
                _ => {}
            }
        } else {
            panic!("ayy");
        }
    }

    fn handle_player_new(&mut self, player_new: PlayerNew) {
        debug!("T={} {:?}", self.tick_index, &player_new);
        self.check_implicit_tick(player_new.cid);
        self.active_sequences.insert(
            player_new.cid,
            DDNetSequence::new(player_new.cid, self.tick_index),
        );
        self.current_tick.add_init_position(player_new);
    }

    fn handle_player_diff(&mut self, player_diff: PlayerDiff) {
        trace!("T={} {:?}", self.tick_index, &player_diff);
        self.check_implicit_tick(player_diff.cid);
        self.current_tick.apply_position_diff(player_diff);
    }

    fn complete_active_sequence(&mut self, cid: i32) {
        let mut sequence = self
            .active_sequences
            .remove(&cid)
            .expect("no active sequence for cid found");

        sequence.end_tick = Some(self.tick_index);
        sequence.player_name = Some(self.player_names.get(&cid).unwrap().clone());

        self.previous_ticks
            .iter()
            .skip((sequence.start_tick) as usize)
            .take((sequence.end_tick.unwrap() - sequence.start_tick) as usize)
            .for_each(|tick| {
                let input_vector = tick.input_vectors.get(&cid);

                // after the first position event there can
                // be a delay until the first actual inputs
                // FIXME: this feels like a dirty hotfix
                if input_vector.is_none() {
                    sequence.start_tick += 1;
                    return;
                }

                sequence.input_vectors.push(
                    *tick
                        .input_vectors
                        .get(&cid)
                        .expect("No input vector found for cid"),
                );
                sequence.player_positions.push(
                    *tick
                        .player_positions
                        .get(&cid)
                        .expect("No player position found for cid"),
                );
            });

        self.completed_sequences.push(sequence);
    }

    fn handle_player_old(&mut self, player_old: PlayerOld) {
        debug!("T={} {:?}", self.tick_index, &player_old);
        self.check_implicit_tick(player_old.cid);
        self.current_tick.remove_player_position(player_old.cid);
        self.complete_active_sequence(player_old.cid);
    }

    // a tick is implicit [...] when a player with lower cid is
    // recorded using any of PLAYER_DIFF, PLAYER_NEW, PLAYER_OLD
    // source: https://ddnet.org/libtw2-doc/teehistorian/
    // INFO: i believe the docs are wrong, and its lower or equal(!) cid
    fn check_implicit_tick(&mut self, cid: i32) {
        if cid <= self.last_cid {
            self.handle_tick_skip(TickSkip { dt: 0 });
        }
        self.last_cid = cid;
    }

    fn handle_console_command(&mut self, command: ConsoleCommand) {
        if command.cid == -1 {
            return; // ignore server commands
        }
        let cmd = String::from_utf8_lossy(&command.cmd);
        let args: Vec<String> = command
            .args
            .iter()
            .map(|arg| String::from_utf8_lossy(arg).to_string())
            .collect();
        debug!(
            "T={} CONSOLE COMMAND cid={}, cmd={} args={}",
            self.tick_index,
            command.cid,
            cmd,
            args.join(" ")
        );
    }

    fn handle_eos(&mut self) {
        self.finished = true;
        let cids: Vec<i32> = self.active_sequences.keys().cloned().collect();
        for cid in cids {
            self.complete_active_sequence(cid);
        }
        debug!("T={} EOS", self.tick_index);
    }

    fn handle_drop(&mut self, drop: Drop) {
        debug!("T={} {:?}", self.tick_index, &drop);
        self.current_tick.input_vectors.remove(&drop.cid);
        // we dont clear player position, as this is handled by OldPlayer event
    }

    pub fn parse_chunk(&mut self, chunk: Chunk) {
        assert!(
            !self.finished,
            "parser already finished, EOS chunk was reached"
        );

        match chunk {
            Chunk::TickSkip(skip) => self.handle_tick_skip(skip),
            Chunk::InputNew(inp_new) => self.handle_input_new(inp_new),
            Chunk::InputDiff(inp_diff) => self.handle_input_diff(inp_diff),
            Chunk::NetMessage(net_msg) => self.handle_net_message(net_msg),
            Chunk::PlayerDiff(player_diff) => self.handle_player_diff(player_diff),
            Chunk::Eos => self.handle_eos(),
            Chunk::ConsoleCommand(command) => self.handle_console_command(command),
            Chunk::PlayerOld(player) => self.handle_player_old(player),
            Chunk::PlayerNew(player) => self.handle_player_new(player),
            Chunk::Drop(drop) => self.handle_drop(drop),
            Chunk::PlayerReady(rdy) => debug!("T={} {:?}", self.tick_index, rdy),
            Chunk::Join(join) => debug!("T={} {:?}", self.tick_index, join),
            // ignore these
            Chunk::JoinVer6(_) | Chunk::JoinVer7(_) | Chunk::DdnetVersion(_) => {}
            _ => {
                warn!(
                    "chunk={}, tick={} -> Untracked Chunk Variant: {:?}",
                    self.chunk_index, self.tick_index, chunk
                );
            }
        }

        self.chunk_index += 1;
    }
}
