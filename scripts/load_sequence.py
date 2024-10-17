import json
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from matplotlib.collections import LineCollection

# for i in range(800):
#     try:
#         with open(f"data/out/output_{i}.json") as file:
#             data = json.load(file)
#             if data["player_name"] == "iMilchshake":
#                 print(i, data["player_name"],
#                       data["end_tick"] - data["start_tick"])
#     except Exception as e:
#         print(f"Error with file output_{i}.json: {e}")

# Load data
with open("data/out/output_608.json") as file:
    data = json.load(file)

print(data["input_vectors"][0])

exit(0)

MAX_TICKS = 5000
TICKS_PER_SECOND = 50
TICK_SKIP = 1

print(data["player_name"])


# Extract player positions
player_x_positions, player_y_positions = np.array(
    [position for position in data["player_positions"][:MAX_TICKS]]).T

# Compute cursor positions
cursor_x_positions = player_x_positions + \
    np.array([input_vector[1]
             for input_vector in data["input_vectors"][:MAX_TICKS]])
cursor_y_positions = player_y_positions + \
    np.array([input_vector[2]
             for input_vector in data["input_vectors"][:MAX_TICKS]])

# Extract hook status
hook_enabled = [input_vector[5]
                for input_vector in data["input_vectors"][:MAX_TICKS]]

# Prepare line segments
line_segments_data = [
    ((player_x_positions[i], player_y_positions[i]),
     (cursor_x_positions[i], cursor_y_positions[i]))
    if hook_enabled[i] else ((player_x_positions[i], player_y_positions[i]), (player_x_positions[i], player_y_positions[i]))
    for i in range(MAX_TICKS)
]

# Setup plot
figure, axis = plt.subplots(figsize=(8, 2))
axis.invert_yaxis()
axis.set_aspect('equal')
axis.grid(True)
axis.set(title='Player Positions',
         xlabel='X Coordinates', ylabel='Y Coordinates')

# Set axes limits
axis.set_xlim(min(player_x_positions.min(), cursor_x_positions.min()), max(
    player_x_positions.max(), cursor_x_positions.max()))
axis.set_ylim(max(player_y_positions.max(), cursor_y_positions.max()), min(
    player_y_positions.min(), cursor_y_positions.min()))  # Inverted y-axis

# Initialize plot elements
line_collection = LineCollection(
    [], colors='gray', linewidths=0.5, alpha=0.5, label="hook")
axis.add_collection(line_collection)
player_scatter, = axis.plot(
    [], [], 'o', color='black', markersize=2, label="player")
cursor_scatter, = axis.plot(
    [], [], 'o', color='lime', markersize=2, label="cursor")

# Update function for animation


def update(frame_number):
    print(frame_number)
    player_scatter.set_data(
        player_x_positions[:frame_number], player_y_positions[:frame_number])
    cursor_scatter.set_data(
        cursor_x_positions[:frame_number], cursor_y_positions[:frame_number])
    line_collection.set_segments(line_segments_data[:frame_number])
    return player_scatter, cursor_scatter, line_collection


# Create animation
animation = FuncAnimation(
    figure, update, frames=range(1, MAX_TICKS + 1, 1 + TICK_SKIP), interval=(1000/TICKS_PER_SECOND))
plt.legend()
plt.tight_layout()
plt.show()
