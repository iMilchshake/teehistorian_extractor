use crate::extractor::SimpleSequence;
use std::collections::HashMap;

pub fn extract_sub_sequences(
    sequence: &SimpleSequence,
    durations: Vec<(usize, usize)>,
) -> Vec<SimpleSequence> {
    let mut sub_sequences = Vec::new();

    for (start, end) in durations {
        assert!(start < sequence.tick_count && end < sequence.tick_count);

        let sub_sequence = SimpleSequence {
            start_tick: sequence.start_tick + start,
            tick_count: end - start + 1,
            pos_x: sequence.pos_x[start..=end].to_vec(),
            pos_y: sequence.pos_y[start..=end].to_vec(),
            move_dir: sequence.move_dir[start..=end].to_vec(),
            target_x: sequence.target_x[start..=end].to_vec(),
            target_y: sequence.target_y[start..=end].to_vec(),
            jump: sequence.jump[start..=end].to_vec(),
            fire: sequence.fire[start..=end].to_vec(),
            hook: sequence.hook[start..=end].to_vec(),
            player_name: sequence.player_name.clone(),
            map_name: sequence.map_name.clone(),
        };

        sub_sequences.push(sub_sequence);
    }

    sub_sequences
}

pub fn pad_durations(
    durations: Vec<(usize, usize)>,
    max_tick: usize,
    margin: usize,
) -> Vec<(usize, usize)> {
    let mut adjusted_durations: Vec<(usize, usize)> = Vec::new();

    for (i, &(start, end)) in durations.iter().enumerate() {
        let mut adjusted_start = if start >= margin { start - margin } else { 0 };
        let mut adjusted_end = if end + margin <= max_tick {
            end + margin
        } else {
            max_tick
        };

        // Check if there is a next duration to avoid overlapping
        if let Some(&(next_start, _)) = durations.get(i + 1) {
            if adjusted_end >= next_start {
                adjusted_end = next_start - 1; // Limit padding to avoid overlap
            }
        }

        // Check for overlap with the previous adjusted duration and limit the start
        if let Some(&(_, prev_end)) = adjusted_durations.last() {
            if adjusted_start <= prev_end {
                adjusted_start = prev_end + 1; // Limit start to avoid overlap
            }
        }

        // Ensure valid interval after adjustments
        if adjusted_start <= adjusted_end {
            adjusted_durations.push((adjusted_start, adjusted_end));
        }
    }

    adjusted_durations
}

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

pub fn get_top_k_players(
    sequences: &[SimpleSequence],
    k: usize,
    count_ticks: bool,
) -> Vec<(String, usize)> {
    let mut player_counts: HashMap<&str, usize> = HashMap::new();
    for sequence in sequences {
        let increment = if count_ticks { sequence.tick_count } else { 1 };
        *player_counts.entry(&sequence.player_name).or_insert(0) += increment;
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
