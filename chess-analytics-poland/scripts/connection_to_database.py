import requests
import psycopg2
import psycopg2.extras
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_PARAMS = {
    "host": "localhost",
    "database": "chess_data",
    "user": "postgres",
    "password": "gharm",
}

HEADERS = {'User-Agent': 'QueenIsBeautiful (your_email@example.com)'}

PLAYER = "hikaru"
ARCHIVES_URL = f"https://api.chess.com/pub/player/{PLAYER}/games/archives"

def get_db_connection():
    """Establish a connection to PostgreSQL."""
    try:
        return psycopg2.connect(**DB_PARAMS)
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        return None

def fetch_all_games_urls():
    """Fetch all available games archive URLs dynamically."""
    try:
        response = requests.get(ARCHIVES_URL, headers=HEADERS)
        response.raise_for_status()
        archives = response.json().get("archives", [])

        if not archives:
            logging.error("No archives found for the player.")
            return []

        logging.info(f"Found {len(archives)} archive(s) for player {PLAYER}")
        return archives  # Return all archive URLs

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch archives: {e}")
        return []

def fetch_games_data(games_url):
    """Fetch game data from a specific archive URL."""
    try:
        response = requests.get(games_url, headers=HEADERS)
        response.raise_for_status()
        return response.json().get("games", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"API request error: {e}")
        return []

def insert_players(players, cur):
    """Insert multiple players using batch execution."""
    query = """
        INSERT INTO players (player_id, username, rating)
        VALUES (%s, %s, %s)
        ON CONFLICT (player_id) DO NOTHING
    """
    try:
        psycopg2.extras.execute_batch(cur, query, players)
        logging.info(f"Inserted {len(players)} players")
    except psycopg2.Error as e:
        logging.error(f"Failed to insert players: {e}")

def insert_games(games, cur):
    """Insert multiple games using batch execution."""
    query = """
        INSERT INTO games (game_id, white_player_id, black_player_id, white_rating, black_rating, 
                           time_class, time_control, rules, pgn, start_time, winner)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_id) DO NOTHING
    """
    try:
        psycopg2.extras.execute_batch(cur, query, games)
        logging.info(f"Inserted {len(games)} games")
    except psycopg2.Error as e:
        logging.error(f"Failed to insert games: {e}")

def main():
    """Main function to fetch and insert all games into PostgreSQL."""
    conn = get_db_connection()
    if not conn:
        return

    cur = conn.cursor()

    # Fetch all archive URLs
    all_games_urls = fetch_all_games_urls()
    if not all_games_urls:
        logging.error("No games archives found.")
        return

    players = set()
    games = []

    for games_url in all_games_urls:
        logging.info(f"Fetching games from {games_url}...")
        games_data = fetch_games_data(games_url)
        if not games_data:
            logging.info(f"No games found in archive: {games_url}")
            continue

        for game in games_data:
            try:
                for color in ["white", "black"]:
                    player = game[color]
                    players.add((
                        player["@id"].split("/")[-1],  # Extracts player ID from API URL
                        player["username"],
                        player.get("rating", 0)  # Default rating to 0 if missing
                    ))

                # Extract game data
                game_id = game.get("uuid", game["url"].split("/")[-1])  # Fallback to game URL if UUID missing
                winner = game["white"]["username"] if game["white"]["result"] == "win" else game["black"]["username"]

                games.append((
                    game_id,
                    game["white"]["@id"].split("/")[-1],
                    game["black"]["@id"].split("/")[-1],
                    game["white"]["rating"],
                    game["black"]["rating"],
                    game["time_class"],
                    game["time_control"],
                    game["rules"],
                    game["pgn"],
                    time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(game["end_time"])),
                    winner
                ))

            except KeyError as e:
                logging.warning(f"Skipping game due to missing key: {e}")

    insert_players(list(players), cur)
    insert_games(games, cur)

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Database update complete!")

if __name__ == "__main__":
    main()
