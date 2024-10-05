import json
from matplotlib import pyplot as plt

file_path = "data/out/output_1.json"

with open(file_path, 'r') as file:
    data: dict = json.load(file)


print(data.keys())
print(data["player_positions"])
print(len(data["player_positions"]))


x_coords, y_coords = zip(*data["player_positions"])
plt.figure(figsize=(8, 2))
plt.scatter(x_coords, y_coords, alpha=0.5, edgecolor='black', s=0.1)
plt.title('Player Positions')
plt.xlabel('X Coordinates')
plt.ylabel('Y Coordinates')
plt.grid(True)
plt.gca().set_aspect('equal', adjustable='box')
plt.tight_layout()
plt.savefig("out.png", dpi=600)
plt.show()
