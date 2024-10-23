import pyarrow as pa
import pyarrow.ipc as ipc
import pandas as pd
import time


def load_arrow_file(file_path: str) -> pd.DataFrame:
    with pa.memory_map(file_path, "r") as source:
        reader = ipc.RecordBatchFileReader(source)
        table = reader.read_all()
    return table.to_pandas()


if __name__ == "__main__":
    t0 = time.time()

    df = load_arrow_file("data/out/sequences.arrow")

    print(f"Arrow file loaded in {time.time() - t0:.2f} seconds.")
    print("Loaded Data:")
    print(df.head())  # Display the first few rows of the data
    print(f"Total rows loaded: {len(df)}")
