import time
import msgpack
import torch
from torch.utils.data import Dataset, DataLoader
from typing import Tuple, List


class SequenceDataset(Dataset):
    def __init__(self, data):
        self.data = data
        self.max_length = max(len(sequence[1]) for sequence in data)

    @classmethod
    def from_file(cls, file_path: str) -> "SequenceDataset":
        data = cls._parse_msgpack(file_path)
        return cls(data)

    @staticmethod
    def _parse_msgpack(
        file_path: str,
    ) -> List:
        with open(file_path, "rb") as f:
            data = msgpack.unpackb(f.read(), raw=False)
        return data

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, idx: int) -> Tuple[int, torch.Tensor, str]:
        sequence = self.data[idx]
        start_tick = sequence[0]
        ticks = sequence[1]
        player_name = sequence[2]

        # Convert ticks data to tensor
        ticks_tensor = torch.tensor(ticks, dtype=torch.float32)

        # Pad by repeating the last tick until the max_length is reached
        num_ticks = ticks_tensor.shape[0]
        if num_ticks < self.max_length:
            padding = ticks_tensor[-1:].repeat(self.max_length - num_ticks, 1)
            ticks_tensor = torch.cat([ticks_tensor, padding], dim=0)

        return start_tick, ticks_tensor, player_name


# Create the dataset from the file using the factory method
t0 = time.perf_counter()
dataset = SequenceDataset.from_file("data/out/all_sequences.msgpack")
print(f"t={time.perf_counter() - t0:.2f} sec")
print(f"N={len(dataset)}")

# Use the dataset with a DataLoader
dataloader = DataLoader(dataset, batch_size=32, shuffle=True)

# Iterate through the DataLoader
for start_tick, ticks_tensor, player_name in dataloader:
    print(f"Start Tick: {start_tick}")
    print(f"Ticks Tensor: {ticks_tensor.shape}")
    print(f"Player Names: {player_name}")
