import json
import os
import re
from collections import Counter

PLAYER_NAME = 'hikaru'
DATA_DIR = PLAYER_NAME
OUTPUT_FILE = f"{DATA_DIR}/{PLAYER_NAME}_analysis.json"

total_games = 0
results = Counter()
opening_moves = Counter()
opponents = Counter()
game_lengths = []


for filename in os.listdir(DATA_DIR):
    if filename.endswith(".json") and "analysis" not in filename:  
        with open(os.path.join(DATA_DIR, filename), 'r', encoding='utf-8') as f:
            data = json.load(f)

            for game in data.get("games", []):
                total_games += 1

                # Get game result
                white = game['white']['username']
                black = game['black']['username']
                result = game['white']['result'] if white == PLAYER_NAME else game['black']['result']

                results[result] += 1
                opponents[white if black == PLAYER_NAME else black] += 1

                # Extract first move safely
                if "pgn" in game:
                    moves = re.findall(r"\d+\.\s([^\s]+)", game["pgn"])  # Extracts first move of each turn
                    if moves:
                        opening_moves[moves[0]] += 1

                    game_lengths.append(len(moves))  # Count moves safely


average_game_length = round(sum(game_lengths) / len(game_lengths), 2) if game_lengths else 0
most_common_opening = opening_moves.most_common(1)
most_frequent_opponent = opponents.most_common(1)

analysis_data = {
    "total_games": total_games,
    "results": results,
    "average_game_length": average_game_length,
    "most_common_opening": most_common_opening[0] if most_common_opening else None,
    "most_frequent_opponent": most_frequent_opponent[0] if most_frequent_opponent else None,
}

with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    json.dump(analysis_data, f, indent=4)

print(f"Analysis saved to {OUTPUT_FILE}")
