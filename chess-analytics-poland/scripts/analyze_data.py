import pandas as pd
from sqlalchemy import create_engine

# Database connection
DB_URL = "postgresql://postgres:gharm@localhost:5432/chess_data"
engine = create_engine(DB_URL)

query = """
SELECT 
    white_player_id, 
    AVG(white_rating) AS avg_white_rating, 
    black_player_id, 
    AVG(black_rating) AS avg_black_rating
FROM games
GROUP BY white_player_id, black_player_id
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT 
    white_player_id, COUNT(*) AS white_games, 
    black_player_id, COUNT(*) AS black_games
FROM games
GROUP BY white_player_id, black_player_id
"""
df = pd.read_sql(query, engine)
print(df)

query = """
SELECT 
    winner, 
    COUNT(*) AS total_games,
    SUM(CASE WHEN winner = white_player_id THEN 1 ELSE 0 END) AS white_wins,
    SUM(CASE WHEN winner = black_player_id THEN 1 ELSE 0 END) AS black_wins
FROM games
GROUP BY winner
"""
df = pd.read_sql(query, engine)
df['white_win_rate'] = df['white_wins'] / df['total_games']
df['black_win_rate'] = df['black_wins'] / df['total_games']
print(df)
