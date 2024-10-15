import teehistorian_extractor
import time

# Start time
start_time = time.time()

# Call the function
data = teehistorian_extractor.get_simplified_ticks("data/random/")

# End time
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

# Print the first element of the result and the time taken
print(f"Time taken: {elapsed_time:.6f} seconds")
