import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Database connection
DB_URL = "postgresql://postgres:gharm@localhost:5432/chess_data"
engine = create_engine(DB_URL)

# Log setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def update_games_table_with_dates():
    try:
        csv_path = r"c:/Users/ksawe/OneDrive/Desktop/Chess.com Polish Grandmasters/chess-analytics-poland/data/extracted_dates.csv"
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
        with engine.connect() as connection:
            for index, row in df_dates.iterrows():
                game_id = row['game_id']
                date_time = row['date_t']
                connection.execute(text("""
                    UPDATE games
                    SET date_t = :date_t 
                    WHERE game_id = :game_id;
                """), {"date_time": date_time, "game_id": game_id})
            connection.commit()

        logging.info("Successfully updated games table with date_t data.")

    except Exception as e:
        logging.error(f"Error updating games table: {e}")

if __name__ == "__main__":
    update_games_table_with_dates()