import teehistorian_extractor
from teehistorian_extractor import SimpleSequence, SimplifiedTick
import time


# Start time
start_time = time.time()

# Call the function
simple_seqs = teehistorian_extractor.get_simplified_ticks("data/random/")


# End time
end_time = time.time()

test = simple_seqs[0].start_tick

print(simple_seqs[0])

print(simple_seqs[0].ticks)
tick: list[SimplifiedTick] = simple_seqs[0].ticks
print(tick[0].fire)
print(type(simple_seqs), type(simple_seqs[0]))

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the first element of the result and the time taken
print(f"Time taken: {elapsed_time:.6f} seconds")
