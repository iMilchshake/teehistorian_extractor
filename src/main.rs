use core::str;
use derivative::Derivative;
use log::{debug, error, info, warn};
use serde::Deserialize;
use serde_json::from_str;
use std::fs;
use std::{collections::HashMap, fs::File};
use teehistorian::chunks::{
    ConsoleCommand, Drop, InputDiff, InputNew, NetMessage, PlayerDiff, PlayerNew, PlayerOld,
    TickSkip,
};
use teehistorian::{Chunk, Th, ThBufReader};
use twgame_core::net_msg::{self, Team};

// https://gitlab.com/ddnet-rs/twgame/-/blob/594f3f4869d34d0382ecceeaeb52cf81853ade7c/twgame-core/src/lib.rs#L93
//     direction: input[0],
//     target_x: input[1],
//     target_y: input[2],
//     jump: input[3],
//     fire: input[4],
//     hook: input[5],
//     player_flags: input[6], // range 0 - 256
//     wanted_weapon: input[7],
//     next_weapon: input[8],
//     prev_weapon: input[9],

#[derive(Debug, Deserialize)]
struct GameInfo {
    server_name: String,
    map_name: String,
}

impl GameInfo {
    fn from_header_bytes(header_bytes: &[u8]) -> Self {
        let header_str =
            str::from_utf8(header_bytes).expect("failed to convert header_bytes to utf-8");
        let game_info = from_str(header_str).expect("failed to extract GameInfo from header_str");
        game_info
    }
}

#[derive(Debug, Clone)]
struct Player {
    name: String,
}

impl Player {
    fn new(name: String) -> Player {
        Player { name }
    }
}

/// A tick defines the input vectors and player positions for a timestep.
/// With the exception of the first tick, the previous tick is copied during
/// parsing and only the changes are applied. This means that after successful parsing,
/// all implicit information is explicitly available for each tick.
#[derive(Clone, Debug)]
struct Tick {
    /// tracks input vectors for each cid
    input_vectors: HashMap<i32, [i32; 10]>,

    /// tracks player position for each cid (x, y)
    player_positions: HashMap<i32, (i32, i32)>,

    /// tracks player information for each cid
    players: HashMap<i32, Player>,
}

impl Tick {
    /// initializes an empty tick struct
    fn new() -> Tick {
        Tick {
            input_vectors: HashMap::new(),
            player_positions: HashMap::new(),
            players: HashMap::new(),
        }
    }

    /// Add inital player position based on PlayerNew chunk
    fn add_init_position(&mut self, new_player: PlayerNew) {
        assert!(!self.player_positions.contains_key(&new_player.cid));
        self.player_positions
            .insert(new_player.cid, (new_player.x, new_player.y));
    }

    /// Add initial player input based on PlayerNew chunk
    fn add_init_input(&mut self, input_new: InputNew) {
        if self.input_vectors.contains_key(&input_new.cid) {
            warn!(
                "OVERWRITE: for cid={} an input vector exists={:?}, new input vector={:?}",
                &input_new.cid,
                self.input_vectors.get(&input_new.cid).unwrap(),
                input_new.input
            );
            // FIXME: remove input vector once a player actually leaves the server?
            panic!();
        }
        self.input_vectors.insert(input_new.cid, input_new.input);
    }

    /// Update tick's input vector for some cid given a InputDiff
    fn apply_input_diff(&mut self, input_diff: InputDiff) {
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
    fn apply_position_diff(&mut self, player_diff: PlayerDiff) {
        let position = self
            .player_positions
            .get_mut(&player_diff.cid)
            .expect("no position for cid exists yet");

        position.0 += player_diff.dx;
        position.1 += player_diff.dy;
    }

    /// remove player position for PlayerOld events
    fn remove_player_position(&mut self, cid: i32) {
        self.player_positions
            .remove(&cid)
            .expect("no position for cid exists");

        // for input vectors we dont expect/assert the removal, as its possible
        // that no input vector has been set (e.g. join and insta leave)
        // self.input_vectors.remove(&cid); FIXME: remove?
    }
}

/// tracks state while parsing teehistorian file
struct Parser {
    /// tracks if end of stream (EOS) chunk has already been parsed
    finished: bool,

    /// tracks tick
    tick_index: i32,

    /// tracks chunk index
    chunk_index: u32,

    /// tracks last seen cid in a player event
    last_cid: i32,

    /// tracks current tick
    current_tick: Tick,

    /// tracks all previous ticks
    previous_ticks: Vec<Tick>,

    /// tracks all active sequences
    active_sequences: HashMap<i32, PlayerSequence>,

    /// tracks all completed sequences
    completed_sequences: Vec<PlayerSequence>,
}

impl Parser {
    fn new() -> Parser {
        Parser {
            finished: false,
            tick_index: 0,
            chunk_index: 0,
            last_cid: -1, // can never be larger or equal to first cid
            current_tick: Tick::new(),
            previous_ticks: Vec::new(),
            active_sequences: HashMap::new(),
            completed_sequences: Vec::new(),
        }
    }

    /// Skips dt+1 ticks. In the case of dt=0 this just "finalizes" the current tick
    fn handle_tick_skip(&mut self, skip: TickSkip) {
        self.tick_index += 1 + skip.dt;
        for _ in 0..(skip.dt + 1) {
            self.previous_ticks.push(self.current_tick.clone());
        }

        if skip.dt > 0 {
            debug!("Skipped {} ticks", 1 + skip.dt);
        }

        debug!("tick={}\t{:?}", self.tick_index, &self.current_tick);
    }

    fn handle_input_new(&mut self, input_new: InputNew) {
        info!("T={} {:?}", self.tick_index, &input_new);
        self.current_tick.add_init_input(input_new);
    }

    fn handle_input_diff(&mut self, input_diff: InputDiff) {
        debug!("T={} {:?}", self.tick_index, &input_diff);
        self.current_tick.apply_input_diff(input_diff);
    }

    fn handle_net_message(&mut self, net_msg: NetMessage) {
        let res = net_msg::parse_net_msg(&net_msg.msg, &mut net_msg::NetVersion::V06);
        if let Ok(res) = res {
            match res {
                net_msg::ClNetMessage::ClStartInfo(info) => {
                    let player_name = String::from_utf8_lossy(info.name);
                    info!("StartInfo cid={} => name={}", net_msg.cid, player_name);
                    self.current_tick
                        .players
                        .insert(net_msg.cid, Player::new(player_name.to_string()));
                }
                net_msg::ClNetMessage::ClKill => {
                    debug!("tick={} cid={} KILL", self.tick_index, net_msg.cid);
                }
                net_msg::ClNetMessage::ClSetTeam(team) => match team {
                    Team::Spectators => {
                        info!("cid={} to spec", net_msg.cid);
                    }
                    Team::Red | Team::Blue => {
                        info!("cid={} to red/blue", net_msg.cid);
                    }
                },
                _ => {}
            }
        } else {
            panic!("ayy");
        }
    }

    fn handle_player_new(&mut self, player_new: PlayerNew) {
        info!("T={} {:?}", self.tick_index, &player_new);
        self.check_implicit_tick(player_new.cid);
        self.active_sequences.insert(
            player_new.cid,
            PlayerSequence::new(player_new.cid, self.tick_index),
        );
        self.current_tick.add_init_position(player_new);
    }

    fn handle_player_diff(&mut self, player_diff: PlayerDiff) {
        debug!("T={} {:?}", self.tick_index, &player_diff);
        self.check_implicit_tick(player_diff.cid);
        self.current_tick.apply_position_diff(player_diff);
    }

    fn complete_active_sequence(&mut self, cid: i32) {
        let mut sequence = self
            .active_sequences
            .remove(&cid)
            .expect("no active sequence for cid found");
        sequence.end_tick = Some(self.tick_index);
        sequence.ticks = Some(
            self.previous_ticks[sequence.start_tick as usize..self.tick_index as usize].to_vec(),
        );

        // The last two ticks of a sequence are Drop followed by PlayerOld, so we can access the
        // last name used by the player by accessing the tick before the Drop tick.
        // FIXME: can i just do a temporary "name registry" which only stores the most recent name
        // for each cid, but isnt tracked for each tick?
        sequence.player_name = Some(
            sequence.ticks.as_ref().unwrap()[sequence.ticks.as_ref().unwrap().len() - 3]
                .players
                .get(&cid)
                .unwrap()
                .name
                .clone(),
        );
        self.completed_sequences.push(sequence);
    }

    fn handle_player_old(&mut self, player_old: PlayerOld) {
        info!("T={} {:?}", self.tick_index, &player_old);
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
        debug!("cid={}, cmd={} args={}", command.cid, cmd, args.join(" "));
    }

    fn handle_eos(&mut self) {
        self.finished = true;
        let cids: Vec<i32> = self.active_sequences.keys().cloned().collect();
        for cid in cids {
            self.complete_active_sequence(cid);
        }
    }

    fn handle_drop(&mut self, drop: Drop) {
        info!("T={} {:?}", self.tick_index, &drop);
        self.current_tick.players.remove(&drop.cid);
        self.current_tick.input_vectors.remove(&drop.cid);
        // we dont clear player position, as this is handled by OldPlayer event
    }

    fn parse_chunk(&mut self, chunk: Chunk) {
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
            // ignore these
            Chunk::JoinVer6(_)
            | Chunk::JoinVer7(_)
            | Chunk::Join(_)
            | Chunk::DdnetVersion(_)
            | Chunk::PlayerReady(_) => {}
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
#[derive(Derivative)]
#[derivative(Debug)]
struct PlayerSequence {
    cid: i32,
    start_tick: i32,
    end_tick: Option<i32>,
    player_name: Option<String>,
    #[derivative(Debug = "ignore")]
    ticks: Option<Vec<Tick>>,
}

impl PlayerSequence {
    fn new(cid: i32, start_tick: i32) -> PlayerSequence {
        PlayerSequence {
            cid,
            start_tick,
            end_tick: None,
            player_name: None,
            ticks: None,
        }
    }
}

fn main() {
    colog::default_builder()
        // .filter_level(log::LevelFilter::Debug)
        .init();

    let mut all_sequences: Vec<PlayerSequence> = Vec::new();

    for (file_index, entry) in fs::read_dir("data/random").unwrap().enumerate() {
        let path = entry.unwrap().path();
        info!("{} parsing {}", file_index, path.to_string_lossy());
        let f = File::open(&path).unwrap();
        let mut th = Th::parse(ThBufReader::new(f)).unwrap();

        let game_info = GameInfo::from_header_bytes(th.header().unwrap());
        info!("{:?}", game_info);

        let mut parser = Parser::new();
        while !parser.finished {
            if let Ok(chunk) = th.next_chunk() {
                parser.parse_chunk(chunk);
            } else {
                break;
            }
        }

        info!(
            "parsed {} chunks including {} ticks",
            parser.chunk_index, parser.tick_index
        );

        for sequence in parser.completed_sequences.iter() {
            info!("{:?}", &sequence);
        }

        all_sequences.append(&mut parser.completed_sequences);
    }

    info!("extracted {} sequences", all_sequences.len());

    let total_ticks = all_sequences
        .iter()
        .map(|s| s.end_tick.unwrap() - s.start_tick)
        .sum::<i32>();
    info!(
        "total ticks={} equal to {} minutes of gameplay",
        total_ticks,
        (total_ticks as f32 / (50. * 60.))
    )
}
