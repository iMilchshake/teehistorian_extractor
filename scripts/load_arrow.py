import argparse
import os
import pyarrow as pa
import pyarrow.ipc as ipc
import pandas as pd
import time
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from typing import Iterable


def load_arrow_file(file_path: str) -> "pd.DataFrame":
    t0 = time.time()
    with pa.memory_map(file_path, "r") as source:
        reader = ipc.RecordBatchFileReader(source)
        table = reader.read_all()
    df = table.to_pandas()
    print(f"Arrow file loaded in {time.time() - t0:.2f} seconds.")
    return df


def cli_parse():
    parser = argparse.ArgumentParser(description="Load an Arrow file from a directory")
    parser.add_argument(
        "directory",
        nargs="?",
        default="data/out",
        type=str,
        help="Path to the arrow data directory",
    )

    args = parser.parse_args()
    sequences_path = os.path.join(args.directory, "sequences.arrow")
    ticks_path = os.path.join(args.directory, "ticks.arrow")

    if not os.path.exists(sequences_path):
        raise FileNotFoundError(f"Error: {sequences_path} does not exist.")
    elif not os.path.exists(ticks_path):
        raise FileNotFoundError(f"Error: {ticks_path} does not exist.")

    return sequences_path, ticks_path


def visualize_players(
    ticks_df: pd.DataFrame, sequences_df: pd.DataFrame, sequence_ids: Iterable
):
    plt.figure(figsize=(10, 10))

    # Filter sequences and ticks based on sequence_ids
    filtered_ticks = ticks_df[ticks_df["sequence_id"].isin(sequence_ids)]
    filtered_sequences = sequences_df[sequences_df["sequence_id"].isin(sequence_ids)]

    # Get unique players and assign colors
    unique_players = filtered_sequences["player_name"].unique()
    color_map = {
        player: color for player, color in zip(unique_players, mcolors.TABLEAU_COLORS)
    }

    # Create a single legend entry per player
    for player_name, color in color_map.items():
        plt.plot([], [], marker="o", color=color, label=player_name, linestyle="None")

    for sequence_id, seq_data in filtered_ticks.groupby("sequence_id"):
        player_name = filtered_sequences.loc[
            filtered_sequences["sequence_id"] == sequence_id, "player_name"
        ].values[0]
        # Use gray if color is unavailable
        color = color_map.get(player_name, "gray")
        plt.plot(
            seq_data["pos_x"],
            -seq_data["pos_y"],
            marker="o",
            markersize=1,
            alpha=0.5,
            color=color,
        )

    plt.xlabel("Position X")
    plt.ylabel("Position Y (flipped)")
    plt.title("Player Position Trace")
    plt.gca().set_aspect("equal", adjustable="box")
    plt.grid(True)
    plt.legend(loc="upper left", fontsize="small", title="Players")
    plt.show()


if __name__ == "__main__":
    sequences_path, ticks_path = cli_parse()

    sequences = load_arrow_file(sequences_path).sort_values(
        by="tick_count", ascending=False
    )
    ticks = load_arrow_file(ticks_path)

    sequence_counts = sequences.groupby("map_name")["sequence_id"].nunique()
    sorted_counts = sequence_counts.sort_values(ascending=False)
    print(sorted_counts.head(15))

    sequence_ids = sequences[sequences["map_name"] == "easy-large_spiral--g-n49V4MmA="][
        "sequence_id"
    ]
    print(sequence_ids)

    print(sequences.head(5))
    print(ticks.head(5))

    visualize_players(ticks, sequences, sequence_ids)
