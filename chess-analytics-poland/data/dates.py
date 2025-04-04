import requests
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import logging
import re
import sys
import os
import sys
# Log setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Database connection
DB_URL = "postgresql://postgres:gharm@localhost:5432/chess_data"
engine = create_engine(DB_URL)

# Requests session setup
session = requests.Session()
session.headers.update({'User-Agent': 'QueenIsBeautiful (your_email@example.com)'})

# Function to fetch all game archive URLs for a player
def fetch_all_game_urls(player):
    url = f"https://api.chess.com/pub/player/{player}/games/archives"
    try:
        response = session.get(url)
        response.raise_for_status()
        archives = response.json().get("archives", [])
        logging.info(f"Found {len(archives)} archives for {player}")
        return archives
    except Exception as e:
        logging.error(f"Error fetching archives: {e}")
        return []

# Function to fetch games data from an archive URL
def fetch_games_data(archive_url):
    try:
        response = session.get(archive_url)
        response.raise_for_status()
        return response.json().get("games", [])
    except Exception as e:
        logging.error(f"Error fetching games from {archive_url}: {e}")
        return []

# Function to extract the date from a PGN string
def extract_date_from_pgn(pgn):
    match = re.search(r'\[Date "(\d{4}\.\d{1,2}\.\d{1,2})"\]', pgn, re.IGNORECASE)
    if match:
        try:
            return datetime.datetime.strptime(match.group(1), '%Y.%m.%d').date().strftime('%Y-%m-%d')
        except ValueError as e:
            logging.warning(f"Invalid PGN date: {match.group(1)} — {e}")
    logging.warning("No valid Date tag found in PGN. Using default date.")
    return '1900-01-01'

# Function to save extracted dates to a CSV file
def save_extracted_dates(player):
    archives = fetch_all_game_urls(player)
    if not archives:
        return None

    extracted = []
    for archive_url in archives:
        games = fetch_games_data(archive_url)
        for game in games:
            if "pgn" not in game:
                continue
            game_id = game.get("uuid", game["url"].split("/")[-1])
            date_time = extract_date_from_pgn(game["pgn"])
            extracted.append({"game_id": game_id, "date_time": date_time})

    if extracted:
        df = pd.DataFrame(extracted)
        filename = f"{player}_extracted_dates.csv"
        df.to_csv(filename, index=False)
        logging.info(f"Saved {len(df)} records to {filename}")
        return filename
    else:
        logging.warning("No valid dates extracted.")
        return None

# Function to update the database with extracted dates from the CSV file
def update_games_table_with_dates(csv_path):
    try:
        df_dates = pd.read_csv(csv_path)
        logging.info(f"Loaded {len(df_dates)} extracted dates from '{csv_path}'.")

        # Check if date_time column exists in games table
        with engine.connect() as connection:
            result = connection.execute(text(""" 
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = 'games'
                    AND column_name = 'date_time'
                );
            """)).scalar()

            if not result:
                # Add date_time column to games table if it doesn't exist
                connection.execute(text("""
                    ALTER TABLE games
                    ADD COLUMN date_time DATE;
                """))
                connection.commit()
                logging.info("date_time column added to games table.")
            else:
                logging.info("date_time column already exists in games table.")

            # Update games table with date_time from CSV
            for _, row in df_dates.iterrows():
                game_id = row['game_id']
                date_time = row['date_time']
                connection.execute(text("""
                    UPDATE games
                    SET date_time = :date_time
                    WHERE game_id = :game_id;
                """), {"date_time": date_time, "game_id": game_id})
            connection.commit()

        logging.info(f"Successfully updated games table with date_time data.")

    except Exception as e:
        logging.error(f"Error updating database: {e}")

def main():
    # Get Chess.com username from command-line argument
    if len(sys.argv) > 1:
        player = sys.argv[1]
    else:
        player = input("Enter the Chess.com username: ").strip().lower()

    # Step 1: Extract dates and save them to a CSV
    csv_file = save_extracted_dates(player)
    if csv_file:
        # Step 2: Update the database with the extracted dates
        update_games_table_with_dates(csv_file)
    else:
        logging.error(f"Failed to extract dates for {player}.")


if __name__ == "__main__":
    main()
