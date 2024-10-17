import time
import mmap
import msgpack
import torch
from torch.utils.data import Dataset, DataLoader


def parse_msgpack(file_path: str):
    """Parses the MessagePack data from a file using mmap."""
    with open(file_path, "rb") as f:
        with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
            data = msgpack.unpackb(mm.read(), raw=False)
    return data


class SimpleSequenceDataset(Dataset):
    def __init__(self, data, max_length):
        """
        data: A list where each item represents a SimpleSequence.
              Each sequence is a list [start_tick, ticks, player_name].
        max_length: The length to which all sequences will be padded.
        """
        self.data = data
        self.max_length = max_length

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        """
        Returns a tuple containing the sequence start_tick, padded ticks, and player_name.
        """
        sequence = self.data[idx]
        start_tick = sequence[0]
        ticks = sequence[1]
        player_name = sequence[2]

        # Convert ticks data to tensor
        ticks_tensor = torch.tensor(
            [
                [tick[0], tick[1], tick[2], tick[3], tick[4], tick[5], tick[6], tick[7]]
                for tick in ticks
            ],
            dtype=torch.float32,
        )

        assert ticks_tensor.shape[0] > 0, f"Sequence at index {
            idx} has zero ticks."

        # Pad by repeating the last tick until the max_length is reached
        num_ticks = ticks_tensor.shape[0]
        if num_ticks < self.max_length:
            last_tick = ticks_tensor[-1].unsqueeze(0)
            padding = last_tick.repeat(self.max_length - num_ticks, 1)
            ticks_tensor = torch.cat([ticks_tensor, padding], dim=0)

        return start_tick, ticks_tensor, player_name


# Load the data and determine the maximum sequence length
t0 = time.perf_counter()
data = parse_msgpack("data/out/all_sequences.msgpack")
print(f"t={time.perf_counter() - t0:.2f} sec")
print(f"N={len(data)}")

# Determine the maximum length of ticks in the data
max_length = max(len(sequence[1]) for sequence in data)
print(f"Maximum sequence length: {max_length}")

# Create a dataset and a dataloader with batch_size=1
dataset = SimpleSequenceDataset(data, max_length)
dataloader = DataLoader(dataset, batch_size=1, shuffle=True)

# Example usage: iterate over the dataloader
for start_tick, ticks_tensor, player_name in dataloader:
    print(f"Start Tick: {start_tick}")
    print(f"Ticks Tensor: {ticks_tensor.shape}")  # [1, max_length, 8]
    print(f"Player Names: {player_name}")
    # Perform your training or processing here
