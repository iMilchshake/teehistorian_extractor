use crate::extractor::SimpleSequence;
use std::collections::HashMap;

pub fn get_non_afk_durations(
    sequence: &SimpleSequence,
    tick_threshold: usize,
) -> Vec<(usize, usize)> {
    let mut afk = true;
    let mut first_move_tick: Option<usize> = None;
    let mut last_move_tick: Option<usize> = None;
    let mut durations: Vec<(usize, usize)> = Vec::new();

    for (current_tick, &move_dir) in sequence.move_dir.iter().enumerate() {
        let player_moved = move_dir != 0;

        if player_moved {
            last_move_tick = Some(current_tick);

            if afk {
                // Player was AFK and just started moving
                first_move_tick = Some(current_tick);
                afk = false;
            }
        } else if !afk {
            // Player hasn't moved; check if AFK threshold is exceeded
            if let Some(last_tick) = last_move_tick {
                if current_tick - last_tick > tick_threshold {
                    // AFK threshold exceeded; record the duration
                    if let Some(first_tick) = first_move_tick {
                        durations.push((first_tick, last_tick));
                    }
                    afk = true;
                    first_move_tick = None;
                    last_move_tick = None;
                }
            }
        }
    }

    // Handle the case where the player was moving at the end
    if !afk {
        if let (Some(first_tick), Some(last_tick)) = (first_move_tick, last_move_tick) {
            durations.push((first_tick, last_tick));
        }
    }

    durations
}

pub fn get_top_k_players(sequences: &[SimpleSequence], k: usize) -> Vec<(String, usize)> {
    let mut player_counts: HashMap<&str, usize> = HashMap::new();
    for sequence in sequences {
        *player_counts.entry(&sequence.player_name).or_insert(0) += sequence.tick_count;
    }

    let mut player_counts: Vec<(&&str, &usize)> = player_counts.iter().collect();
    player_counts.sort_by(|a, b| b.1.cmp(a.1)); // Sort by total tick count in descending order

    let top_k_players_set: Vec<(String, usize)> = player_counts
        .iter()
        .take(k)
        .map(|&(player, &count)| (player.to_string(), count))
        .collect();

    top_k_players_set
}
