import argparse
import os
import pyarrow as pa
import pyarrow.ipc as ipc
import pandas as pd
import time
import matplotlib.pyplot as plt
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
    parser = argparse.ArgumentParser(
        description="Load an Arrow file from a directory")
    parser.add_argument(
        "directory",
        nargs="?",
        default="data/out",
        type=str,
        help="Path to the arrow data directory"
    )

    args = parser.parse_args()
    sequences_path = os.path.join(args.directory, "sequences.arrow")
    ticks_path = os.path.join(args.directory, "ticks.arrow")

    if not os.path.exists(sequences_path):
        raise FileNotFoundError(f"Error: {sequences_path} does not exist.")
    elif not os.path.exists(ticks_path):
        raise FileNotFoundError(f"Error: {ticks_path} does not exist.")

    return sequences_path, ticks_path


def visualize_players(ticks_df: pd.DataFrame, sequence_ids: Iterable):
    plt.figure(figsize=(10, 10))
    filtered_df = ticks_df[ticks_df['sequence_id'].isin(sequence_ids)]

    filtered_df = filtered_df.sort_index()

    for _, seq_data in filtered_df.groupby('sequence_id'):
        plt.plot(seq_data['pos_x'], -seq_data['pos_y'],
                 marker='o', markersize=1)

    plt.xlabel("Position X")
    plt.ylabel("Position Y (flipped)")
    plt.title(f"Player Position Trace for sequence_ids: {sequence_ids}")
    plt.gca().set_aspect('equal', adjustable='box')
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    sequences_path, ticks_path = cli_parse()

    sequences = load_arrow_file(sequences_path).sort_values(
        by="tick_count", ascending=False)
    ticks = load_arrow_file(ticks_path)

    sequence_counts = sequences.groupby('map_name')['sequence_id'].nunique()
    sorted_counts = sequence_counts.sort_values(ascending=False)
    print(sorted_counts.head(15))

    sequence_ids = sequences[sequences["map_name"] ==
                             "mainV2_stable-small_s_tight-ob4PZo8m8VY="]["sequence_id"]
    print(sequence_ids)

    print(sequences.head(5))
    print(ticks.head(5))

    visualize_players(ticks, sequence_ids)
