import requests
import pandas as pd
from sqlalchemy import create_engine
import time
import logging
import re
import datetime
from sqlalchemy.exc import IntegrityError

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Database connection
DB_URL = "postgresql://postgres:gharm@localhost:5432/chess_data"
engine = create_engine(DB_URL)

# Chess.com API settings
HEADERS = {'User-Agent': 'QueenIsBeautiful (your_email@example.com)'}
PLAYER = "hikaru"
ARCHIVES_URL = f"https://api.chess.com/pub/player/{PLAYER}/games/archives"

# Create an API session
session = requests.Session()
session.headers.update(HEADERS)


def fetch_all_game_urls():
    """Fetches all archive URLs for the player."""
    try:
        response = session.get(ARCHIVES_URL)
        response.raise_for_status()
        archives = response.json().get("archives", [])
        logging.info(f"Found {len(archives)} archives for player {PLAYER}")
        return archives
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch archives: {e}")
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
    """Extracts the date (YYYY-MM-DD) from the PGN."""
    date_match = re.search(r'\[Date "(\d{4}\.\d{2}\.\d{2})"\]', pgn)
    if date_match:
        try:
            date_str = date_match.group(1)
            date_obj = datetime.datetime.strptime(date_str, '%Y.%m.%d').date()
            return date_obj.strftime('%Y-%m-%d')
        except ValueError as e:
            logging.warning(f"Invalid date format in PGN: {e}")
    logging.warning("No valid date found in PGN, defaulting to '1900-01-01'.")
    return '1900-01-01'  # Default date if not found


def get_existing_game_ids():
    """Fetches existing game IDs from the database."""
    try:
        return pd.read_sql("SELECT game_id FROM games", engine)['game_id'].tolist()
    except Exception as e:
        logging.error(f"Error fetching existing game IDs: {e}")
        return []




def process_games():
    """Fetches, processes, and stores new chess games."""
    all_games_urls = fetch_all_game_urls()
    if not all_games_urls:
        logging.warning("No game archives found.")
        return

    existing_game_ids = set(get_existing_game_ids())
    new_games = []

    for games_url in all_games_urls:
        logging.info(f"Fetching games from {games_url}...")
        games_data = fetch_games_data(games_url)

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

                # Construct the game data, excluding `end_time` since it's not needed
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
        df = pd.DataFrame(new_games)
        try:
            df.to_sql('games', engine, if_exists='append', index=False)
            logging.info(f"Inserted {len(new_games)} new games into the database.")
        except IntegrityError as e:
            logging.error(f"Integrity error inserting games: {e}")
        except Exception as e:
            logging.error(f"Unexpected error inserting games: {e}")
    else:
        logging.warning("No new games to insert.")



if __name__ == "__main__":
    process_games()
