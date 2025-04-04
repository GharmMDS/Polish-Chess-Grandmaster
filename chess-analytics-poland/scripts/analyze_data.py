import pandas as pd
from sqlalchemy import create_engine

# Database connection
DB_URL = "postgresql://postgres:gharm@localhost:5432/chess_data"
engine = create_engine(DB_URL)

# 1️⃣ Average ratings between player pairings
query_avg_ratings = """
SELECT 
    white_player_id, 
    AVG(white_rating) AS avg_white_rating, 
    black_player_id, 
    AVG(black_rating) AS avg_black_rating
FROM games
GROUP BY white_player_id, black_player_id
"""
df_avg_ratings = pd.read_sql(query_avg_ratings, engine)
print("🎯 Average Ratings Per Player Pairing:")
print(df_avg_ratings.head())

# 2️⃣ Total games played by player as white and as black — separately aggregated
query_game_counts = """
SELECT player_id, 
       SUM(games_as_white) AS white_games,
       SUM(games_as_black) AS black_games,
       SUM(games_as_white + games_as_black) AS total_games
FROM (
    SELECT white_player_id AS player_id, COUNT(*) AS games_as_white, 0 AS games_as_black
    FROM games
    GROUP BY white_player_id

    UNION ALL

    SELECT black_player_id AS player_id, 0 AS games_as_white, COUNT(*) AS games_as_black
    FROM games
    GROUP BY black_player_id
) sub
GROUP BY player_id
"""
df_game_counts = pd.read_sql(query_game_counts, engine)
print("\n🎯 Total Games Played Per Player (White & Black):")
print(df_game_counts.head())

# 3️⃣ Win stats per player regardless of color
query_win_rates = """
SELECT 
    player_id,
    SUM(CASE WHEN player_id = white_player_id THEN 1 ELSE 0 END) AS games_as_white,
    SUM(CASE WHEN player_id = black_player_id THEN 1 ELSE 0 END) AS games_as_black,
    SUM(CASE WHEN player_id = winner THEN 1 ELSE 0 END) AS wins
FROM (
    SELECT white_player_id AS player_id, * FROM games
    UNION ALL
    SELECT black_player_id AS player_id, * FROM games
) sub
GROUP BY player_id
"""
df_win_rates = pd.read_sql(query_win_rates, engine)
df_win_rates["total_games"] = df_win_rates["games_as_white"] + df_win_rates["games_as_black"]
df_win_rates["win_rate"] = df_win_rates["wins"] / df_win_rates["total_games"]

print("\n🎯 Win Rates Per Player:")
print(df_win_rates.head())
