import requests
import json
import os
import time
import sys
import pandas as pd
from sqlalchemy import create_engine
import datetime
import re
import logging
from sqlalchemy.exc import IntegrityError

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Database connection
DB_URL = "postgresql://postgres:gharm@localhost:5432/chess_data"
engine = create_engine(DB_URL)

# Chess.com API settings
HEADERS = {'User-Agent': 'QueenIsBeautiful (your_email@example.com)'}

# Create an API session
session = requests.Session()
session.headers.update(HEADERS)

def fetch_all_game_urls(player_name):
    """Fetches all archive URLs for the given player."""
    ARCHIVES_URL = f"https://api.chess.com/pub/player/{player_name}/games/archives"
    try:
        response = session.get(ARCHIVES_URL)
        response.raise_for_status()
        archives = response.json().get("archives", [])
        logging.info(f"Found {len(archives)} archives for player {player_name}")
        return archives
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch archives for {player_name}: {e}")
        return []

def fetch_games_data(archive_url):
    """Fetches game data from a single archive."""
    try:
        response = session.get(archive_url)
        response.raise_for_status()
        return response.json().get("games", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching games from {archive_url}: {e}")
        return []

def extract_date_from_pgn(pgn):
    """Extracts the date (YYYY-MM-DD) from the PGN, handling case and varying digit counts."""
    date_match = re.search(
        r'\[Date "(\d{4}\.\d{1,2}\.\d{1,2})"\]',
        pgn,
        re.IGNORECASE
    )
    if date_match:
        date_str = date_match.group(1)
        try:
            date_obj = datetime.datetime.strptime(date_str, '%Y.%m.%d').date()
            return date_obj.strftime('%Y-%m-%d')
        except ValueError as e:
            logging.warning(f"Invalid date '{date_str}' in PGN: {e}")
    else:
        logging.warning("No Date tag found in PGN.")
    return '1900-01-01'

def get_existing_game_ids():
    """Fetches existing game IDs from the database."""
    try:
        return pd.read_sql("SELECT game_id FROM games", engine)['game_id'].tolist()
    except Exception as e:
        logging.error(f"Error fetching existing game IDs: {e}")
        return []

def process_player_games(player_name):
    """Fetches, processes, and stores new chess games for a given player."""
    logging.info(f"Processing games for player: {player_name}")
    all_games_urls = fetch_all_game_urls(player_name)
    if not all_games_urls:
        logging.warning(f"No game archives found for player {player_name}.")
        return

    existing_game_ids = set(get_existing_game_ids())
    new_games = []

    # Directory to save game data
    data_dir = os.path.join(os.getcwd(), player_name)
    os.makedirs(data_dir, exist_ok=True)

    for games_url in all_games_urls:
        # Check if the archive has already been downloaded
        archive_filename = os.path.join(data_dir, f"{player_name}_games_{games_url.split('/')[-2]}_{games_url.split('/')[-1]}.json")
        if os.path.exists(archive_filename):
            logging.info(f"Archive {archive_filename} already downloaded, skipping...")
            continue
        
        logging.info(f"Fetching games from {games_url}...")
        games_data = fetch_games_data(games_url)

        # Save the archive as a JSON file
        if games_data:
            with open(archive_filename, 'w', encoding='utf-8') as f:
                json.dump(games_data, f, indent=4)
            logging.info(f"Saved games data to {archive_filename}")

        for game in games_data:
            try:
                if "pgn" not in game:
                    continue  # Skip games without PGN

                game_id = game.get("uuid", game["url"].split("/")[-1])
                if game_id in existing_game_ids:
                    continue  # Skip existing games

                white = game["white"]
                black = game["black"]
                winner = white["username"] if white["result"] == "win" else black["username"]
                date_time = extract_date_from_pgn(game["pgn"])

                game_data = {
                    "game_id": game_id,
                    "white_player_id": white["username"],
                    "black_player_id": black["username"],
                    "white_rating": white.get("rating", 0),
                    "black_rating": black.get("rating", 0),
                    "time_class": game["time_class"],
                    "time_control": game["time_control"],
                    "rules": game["rules"],
                    "pgn": game["pgn"],
                    "start_time": datetime.datetime.fromtimestamp(game["end_time"]).strftime('%Y-%m-%d %H:%M:%S') if game.get("end_time") else None,
                    "winner": winner,
                    "date_time": date_time
                }
                new_games.append(game_data)

            except KeyError as e:
                logging.warning(f"Skipping game due to missing key: {e}")

        time.sleep(1)  # Rate limit delay

    if new_games:
        logging.info(f"Inserting {len(new_games)} new games for {player_name} into the database.")
        df = pd.DataFrame(new_games)
        try:
            df.to_sql('games', engine, if_exists='append', index=False)
            logging.info(f"Inserted {len(new_games)} new games for {player_name} into the database.")
        except IntegrityError as e:
            logging.error(f"Integrity error inserting games for {player_name}: {e}")
        except Exception as e:
            logging.error(f"Unexpected error inserting games for {player_name}: {e}")
    else:
        logging.info(f"No new games to insert for {player_name}.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        player_to_fetch = sys.argv[1]
        logging.info(f"Fetching data for player from command line: {player_to_fetch}")
        process_player_games(player_to_fetch)
    else:
        player_to_fetch = input("Enter the Chess.com username to fetch data for: ").strip()
        logging.info(f"Fetching data for player from user input: {player_to_fetch}")
        process_player_games(player_to_fetch)
