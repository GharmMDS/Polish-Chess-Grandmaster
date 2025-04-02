import requests
import json
import os
import time

# Constants
HEADERS = {'User-Agent': 'QueenIsBeautiful (your_email@example.com)'}
PLAYER_NAME = 'hikaru'
ARCHIVES_URL = f'https://api.chess.com/pub/player/{PLAYER_NAME}/games/archives'
DATA_DIR = PLAYER_NAME  


os.makedirs(DATA_DIR, exist_ok=True)


response = requests.get(ARCHIVES_URL, headers=HEADERS)

if response.status_code == 200:
    archive_urls = response.json().get('archives', [])

    for archive_url in archive_urls:
        parts = archive_url.rstrip('/').split('/')
        year_month = f"{parts[-2]}_{parts[-1]}"
        filename = os.path.join(DATA_DIR, f"{PLAYER_NAME}_games_{year_month}.json")

        # Skip downloading if the file already exists
        if os.path.exists(filename):
            print(f"Skipping {filename} (already downloaded)")
            continue

        # Fetch game data
        game_response = requests.get(archive_url, headers=HEADERS)
        
        if game_response.status_code == 200:
            game_data = game_response.json()
            
            # Save data
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(game_data, f, indent=4)

            print(f"Downloaded {filename}")

            # Respect Chess.com API rate limits
            time.sleep(1.2)  # Prevent hitting request limits
        else:
            print(f"Error downloading {archive_url}: {game_response.status_code}")
else:
    print(f"Error fetching archives: {response.status_code}")
