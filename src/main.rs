use core::str;
use log::{debug, info, warn};
use serde::Deserialize;
use serde_json::from_str;
use std::{collections::HashMap, fs::File};
use teehistorian::chunks::{
    ConsoleCommand, InputDiff, InputNew, NetMessage, PlayerDiff, PlayerNew, PlayerOld, TickSkip,
};
use teehistorian::{Chunk, Th, ThBufReader};
use twgame_core::net_msg;

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

/// tracks state while parsing teehistorian file
struct Parser {
    /// tracks if end of stream (EOS) chunk has already been parsed
    finished: bool,

    /// tracks tick
    tick: i32,

    /// tracks chunk index
    chunk_index: u32,

    /// tracks last seen cid in a player event
    last_cid: i32,

    /// tracks input vectors for each cid (for implicit ticks)
    input_vectors: HashMap<i32, [i32; 10]>,

    /// tracks current player sequences for each cid
    active_sequences: HashMap<i32, PlayerSequence>,

    /// stores player sequences that have been finished
    finished_sequences: Vec<PlayerSequence>,
}

impl Parser {
    fn new() -> Parser {
        Parser {
            finished: false,
            tick: 0,
            chunk_index: 0,
            last_cid: 0, // TODO: Option here?
            input_vectors: HashMap::new(),
            active_sequences: HashMap::new(),
            finished_sequences: Vec::new(),
        }
    }

    fn handle_tick_skip(&mut self, skip: TickSkip) {
        self.tick += 1 + skip.dt;
        debug!("> skipped {} ticks", 1 + skip.dt);
    }

    fn handle_input_new(&mut self, inp_new: InputNew) {
        debug!(
            "[{}] cid={} -> new {:?}",
            self.chunk_index, inp_new.cid, inp_new.input
        );
    }

    fn handle_input_diff(&mut self, inp_diff: InputDiff) {
        debug!(
            "[{}, {}] cid={} -> pdiff={:?}",
            self.chunk_index, self.tick, inp_diff.cid, inp_diff.dinput
        );
    }

    fn handle_net_message(&mut self, net_msg: NetMessage) {
        let res = net_msg::parse_net_msg(&net_msg.msg, &mut net_msg::NetVersion::V06);
        if let Ok(res) = res {
            match res {
                net_msg::ClNetMessage::ClStartInfo(info) => {
                    info!(
                        "chunk={}, tick={}, cid={} -> name={}",
                        self.chunk_index,
                        self.tick,
                        net_msg.cid,
                        String::from_utf8_lossy(info.name)
                    );
                }
                net_msg::ClNetMessage::ClKill => {
                    info!("tick={} cid={} KILL", self.tick, net_msg.cid);
                }
                _ => {}
            }
        } else {
            panic!("ayy");
        }
    }

    fn handle_player_diff(&mut self, player_diff: PlayerDiff) {
        debug!(
            "[{}, {} pdiff={:?}",
            self.chunk_index, self.tick, player_diff
        );
        if player_diff.cid <= self.last_cid {
            self.tick += 1;
        }
        self.last_cid = player_diff.cid;
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

    fn handle_player_old(&mut self, player: PlayerOld) {
        info!("LEAVE cid={}", player.cid);
    }

    fn handle_player_new(&mut self, player: PlayerNew) {
        info!("JOIN cid={}", player.cid);
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
            Chunk::Eos => self.finished = true,
            Chunk::ConsoleCommand(command) => self.handle_console_command(command),
            Chunk::PlayerOld(player) => self.handle_player_old(player),
            Chunk::PlayerNew(player) => self.handle_player_new(player),
            // ignore these
            Chunk::JoinVer6(_)
            | Chunk::JoinVer7(_)
            | Chunk::Drop(_)
            | Chunk::Join(_)
            | Chunk::DdnetVersion(_)
            | Chunk::PlayerReady(_) => {}
            _ => {
                warn!(
                    "chunk={}, tick={} -> Untracked Chunk Variant: {:?}",
                    self.chunk_index, self.tick, chunk
                );
            }
        }

        self.chunk_index += 1;
    }
}

struct PlayerSequence {
    player_name: Option<String>,
    start_tick: i32,
    inputs: Vec<[i32; 10]>,
}

fn main() {
    colog::default_builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let f = File::open("data/random/38a7c292-76c7-42c0-bb20-cde7dd6bf373.teehistorian").unwrap();
    // TODO: use ThCompat?
    let mut th = Th::parse(ThBufReader::new(f)).unwrap();

    // TODO: parse json
    let game_info = GameInfo::from_header_bytes(th.header().unwrap());
    info!("{:?}", game_info);

    let mut parser = Parser::new();

    while !parser.finished && parser.tick < 1000 {
        let chunk = th.next_chunk().unwrap();
        parser.parse_chunk(chunk);
    }
}
