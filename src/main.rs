use core::str;
use std::fs::File;
use teehistorian::{Chunk, Th, ThBufReader};

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
    let header_str = str::from_utf8(header_bytes).unwrap();

    for index in 0..1000 {
        let chunk = th.next_chunk().unwrap();

        match chunk {
            Chunk::InputNew(ref inp_new) => {
                // dbg!(inp_new);
            }
            Chunk::InputDiff(ref inp_dif) => {
                // dbg!(inp_dif);
            }
            Chunk::Join(join) => {
                println!("[{}] {:?}", index, join);
            }
            Chunk::JoinVer6(join) => {
                println!("[{}] {:?}", index, join);
            }
            Chunk::JoinVer7(join) => {
                println!("[{}] {:?}", index, join);
            }
            Chunk::NetMessage(ref msg) => {
                let res = net_msg::parse_net_msg(&msg.msg, &mut NetVersion::V06);

                if let Ok(res) = res {
                    match res {
                        net_msg::ClNetMessage::ClStartInfo(info) => {
                            println!(
                                "id={} -> name={}",
                                msg.cid,
                                String::from_utf8_lossy(info.name)
                            )
                        }
                        _ => {}
                    }
                } else {
                    panic!("ayy");
                }
            }

            _ => continue,
        }
    }
}
