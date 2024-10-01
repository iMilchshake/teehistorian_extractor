use core::str;
use std::fs::File;
use teehistorian::{chunks::PlayerOld, Chunk, Th, ThBufReader};

use twgame_core::net_msg::{self, NetVersion};

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

fn main() {
    let f = File::open("data/random/38a7c292-76c7-42c0-bb20-cde7dd6bf373.teehistorian").unwrap();
    // TODO: use ThCompat?
    let mut th = Th::parse(ThBufReader::new(f)).unwrap();

    let header_bytes = th.header().unwrap();
    // TODO: parse json
    let header_str = str::from_utf8(header_bytes).unwrap();

    let mut tick = 0;

    let mut last_cid = 0; // tick is implicit if a lower cid is recorded for PLAYER_X events

    for index in 0..1000 {
        let chunk = th.next_chunk().unwrap();

        match chunk {
            // TODO: a tick is implicit in these messages when a player with lower cid is recorded using any of PLAYER_DIFF, PLAYER_NEW, PLAYER_OLD
            Chunk::TickSkip(skip) => {
                tick += 1 + skip.dt;
                println!("> skipped {} ticks", 1 + skip.dt);
            }
            Chunk::InputNew(inp_new) => {
                println!("[{}] cid={} -> new {:?}", index, inp_new.cid, inp_new.input);
            }
            Chunk::InputDiff(inp_diff) => {
                println!(
                    "[{}, {}] cid={} -> pdiff={:?}",
                    index, tick, inp_diff.cid, inp_diff.dinput
                );
            }
            Chunk::Join(join) => {
                println!("[{}] JOIN cid={}", index, join.cid);
            }
            Chunk::NetMessage(ref net_msg) => {
                let res = net_msg::parse_net_msg(&net_msg.msg, &mut NetVersion::V06);

                if let Ok(res) = res {
                    match res {
                        net_msg::ClNetMessage::ClStartInfo(info) => {
                            println!(
                                "id={} -> name={}",
                                net_msg.cid,
                                String::from_utf8_lossy(info.name)
                            )
                        }
                        _ => {}
                    }
                } else {
                    panic!("ayy");
                }
            }
            Chunk::PlayerDiff(player_diff) => {
                println!("[{}, {} pdiff={:?}", index, tick, player_diff);
                if player_diff.cid <= last_cid {
                    tick += 1;
                }
                last_cid = player_diff.cid;
            }
            Chunk::PlayerNew(player_new) => {
                println!("[{}] PLAYER NEW={:?}", index, player_new);
            }
            Chunk::PlayerOld(player_old) => {
                println!("[{}] PLAYER OLD={:?}", index, player_old);
            }
            _ => {
                println!("[?????] untracked variant: {:?}", chunk);
            }
        }
    }
}
