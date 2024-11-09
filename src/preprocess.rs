use log::warn;

use crate::extractor::SimpleSequence;
use std::collections::HashMap;

pub struct Duration {
    start: usize,
    end: usize,
}

impl Duration {
    pub fn new(start: usize, end: usize) -> Duration {
        if start >= end {
            warn!("start >= end!");
        }
        Duration { start, end }
    }

    /// end is inclusive
    pub fn tick_count(&self) -> usize {
        self.end - self.start + 1
    }

    pub fn cut_duration(&self, target_length: usize) -> Vec<Duration> {
        let sequence_count = self.tick_count() / target_length;
        let mut durations = Vec::with_capacity(sequence_count);

        for idx in 0..sequence_count {
            let start = target_length * idx;
            durations.push(Duration::new(start, start + target_length - 1));
        }

        durations
    }

    pub fn pad_durations(
        durations: Vec<Duration>,
        max_tick: usize,
        margin: usize,
    ) -> Vec<Duration> {
        let mut adjusted_durations: Vec<Duration> = Vec::new();

        for (i, duration) in durations.iter().enumerate() {
            let mut start = if duration.start >= margin {
                duration.start - margin
            } else {
                0
            };
            let mut end = if duration.end + margin <= max_tick {
                duration.end + margin
            } else {
                max_tick
            };

            // Check if there is a next duration to avoid overlapping
            if let Some(next_duration) = durations.get(i + 1) {
                if end >= next_duration.start {
                    end = next_duration.start - 1; // Limit padding to avoid overlap
                }
            }

            // Check for overlap with the previous adjusted duration and limit the start
            if let Some(prev_duration) = adjusted_durations.last() {
                if start <= prev_duration.end {
                    start = prev_duration.end + 1; // Limit start to avoid overlap
                }
            }

            // Ensure valid interval after adjustments
            if start <= end {
                adjusted_durations.push(Duration::new(start, end));
            }
        }

        adjusted_durations
    }

    pub fn get_non_afk_durations(
        sequence: &SimpleSequence,
        tick_threshold: usize,
    ) -> Vec<Duration> {
        let mut afk = true;
        let mut first_move_tick: Option<usize> = None;
        let mut last_move_tick: Option<usize> = None;
        let mut durations: Vec<Duration> = Vec::new();

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
                            durations.push(Duration::new(first_tick, last_tick));
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
                durations.push(Duration::new(first_tick, last_tick));
            }
        }

        durations
    }

    pub fn extract_sub_sequences(
        sequence: &SimpleSequence,
        durations: Vec<Duration>,
    ) -> Vec<SimpleSequence> {
        let mut sub_sequences = Vec::new();

        for duration in durations {
            assert!(duration.start < sequence.tick_count && duration.end < sequence.tick_count);

            let sub_sequence = SimpleSequence {
                start_tick: sequence.start_tick + duration.start,
                tick_count: duration.end - duration.start + 1,
                pos_x: sequence.pos_x[duration.start..=duration.end].to_vec(),
                pos_y: sequence.pos_y[duration.start..=duration.end].to_vec(),
                move_dir: sequence.move_dir[duration.start..=duration.end].to_vec(),
                target_x: sequence.target_x[duration.start..=duration.end].to_vec(),
                target_y: sequence.target_y[duration.start..=duration.end].to_vec(),
                jump: sequence.jump[duration.start..=duration.end].to_vec(),
                fire: sequence.fire[duration.start..=duration.end].to_vec(),
                hook: sequence.hook[duration.start..=duration.end].to_vec(),
                player_name: sequence.player_name.clone(),
                map_name: sequence.map_name.clone(),
            };

            sub_sequences.push(sub_sequence);
        }

        sub_sequences
    }
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
