import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Database connection
DB_URL = "postgresql://postgres:gharm@localhost:5432/chess_data"
engine = create_engine(DB_URL)

# Modified query to include the winner
query_hikaru = """
SELECT 
    white_player_id, 
    white_rating, 
    black_player_id, 
    black_rating,
    winner
FROM games
WHERE white_player_id = 'hikaru' OR black_player_id = 'hikaru'
"""

df_hikaru = pd.read_sql(query_hikaru, engine)

# Check the first few rows of the dataframe
print(df_hikaru.head())

# Check unique values in the 'winner' column
print("Unique values in 'winner' column:", df_hikaru['winner'].unique())

# Count the number of games Hikaru played
total_hikaru_games = len(df_hikaru)

# Count the number of games Hikaru won
hikaru_wins = df_hikaru.apply(
    lambda row: 1 if (row['winner'] == 'hikaru') else 0, axis=1
).sum()

# Calculate Hikaru's win rate
hikaru_win_rate = hikaru_wins / total_hikaru_games if total_hikaru_games > 0 else 0

print(f"Total games Hikaru played: {total_hikaru_games}")
print(f"Hikaru's wins: {hikaru_wins}")
print(f"Hikaru's win rate: {hikaru_win_rate * 100:.2f}%")

# Create two subplots: one for white player ratings, another for black player ratings
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Plot for Hikaru's rating as White
sns.histplot(df_hikaru['white_rating'], kde=True, ax=axes[0], color='blue', bins=30, alpha=0.7)
axes[0].set_title("Hikaru's Rating as White Player")
axes[0].set_xlabel("Rating")
axes[0].set_ylabel("Frequency")

# Plot for Hikaru's rating as Black
sns.histplot(df_hikaru['black_rating'], kde=True, ax=axes[1], color='red', bins=30, alpha=0.7)
axes[1].set_title("Hikaru's Rating as Black Player")
axes[1].set_xlabel("Rating")
axes[1].set_ylabel("Frequency")

# Adjust layout
plt.tight_layout()
plt.show()

# Add a 'win' column based on the actual game result for White and Black separately
df_hikaru['win_white'] = df_hikaru.apply(
    lambda row: 1 if (row['white_player_id'] == 'hikaru' and row['winner'] == 'hikaru') else 0,
    axis=1
)

df_hikaru['win_black'] = df_hikaru.apply(
    lambda row: 1 if (row['black_player_id'] == 'hikaru' and row['winner'] == 'hikaru') else 0,
    axis=1
)

# Overall win count
total_wins = df_hikaru['win_white'].sum() + df_hikaru['win_black'].sum()

# Calculate the win rate relative to total games Hikaru played.
overall_win_rate = total_wins / total_hikaru_games if total_hikaru_games > 0 else 0

print(f"Hikaru's total wins: {total_wins}")
print(f"Hikaru's overall win rate: {overall_win_rate * 100:.2f}%")

# Separate dataframes for wins and losses for White and Black
df_hikaru_win_white = df_hikaru[df_hikaru['win_white'] == 1]
df_hikaru_loss_white = df_hikaru[df_hikaru['win_white'] == 0]

df_hikaru_win_black = df_hikaru[df_hikaru['win_black'] == 1]
df_hikaru_loss_black = df_hikaru[df_hikaru['win_black'] == 0]

# Plot rating distribution for wins and losses for White
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Plot for wins as White
sns.histplot(df_hikaru_win_white['white_rating'], kde=True, color='green', label="Wins as White", ax=axes[0], alpha=0.6)
sns.histplot(df_hikaru_loss_white['white_rating'], kde=True, color='red', label="Losses as White", ax=axes[0], alpha=0.6)
axes[0].set_title("Hikaru's Rating Distribution: Wins vs Losses (White)")
axes[0].set_xlabel("Rating")
axes[0].set_ylabel("Frequency")
axes[0].legend()

# Plot for wins as Black
sns.histplot(df_hikaru_win_black['black_rating'], kde=True, color='blue', label="Wins as Black", ax=axes[1], alpha=0.6)
sns.histplot(df_hikaru_loss_black['black_rating'], kde=True, color='orange', label="Losses as Black", ax=axes[1], alpha=0.6)
axes[1].set_title("Hikaru's Rating Distribution: Wins vs Losses (Black)")
axes[1].set_xlabel("Rating")
axes[1].set_ylabel("Frequency")
axes[1].legend()

# Adjust layout
plt.tight_layout()
plt.show()

# Compare Win Rate for White and Black separately
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Plot for Hikaru's win rate when playing as White
win_rate_white = df_hikaru['win_white'].mean()
axes[0].bar(['White'], [win_rate_white], color='blue')
axes[0].set_title("Hikaru's Win Rate as White Player")
axes[0].set_ylabel("Win Rate")
axes[0].set_ylim(0, 1)

# Plot for Hikaru's win rate when playing as Black
win_rate_black = df_hikaru['win_black'].mean()
axes[1].bar(['Black'], [win_rate_black], color='red')
axes[1].set_title("Hikaru's Win Rate as Black Player")
axes[1].set_ylabel("Win Rate")
axes[1].set_ylim(0, 1)

# Adjust layout
plt.tight_layout()
plt.show()

query = """
SELECT 
    white_player_id, 
    white_rating, 
    black_player_id, 
    black_rating, 
    time_control, 
    end_time,
    pgn
FROM games
WHERE white_player_id = 'hikaru' OR black_player_id = 'hikaru'
"""

df = pd.read_sql(query, engine)

# Filter the data for games where Hikaru is either white or black
df_hikaru = df[(df['white_player_id'] == 'hikaru') | (df['black_player_id'] == 'hikaru')]

# Add a 'win' column based on the actual game result for White and Black separately
df_hikaru['win_white'] = df_hikaru.apply(
    lambda row: 1 if row['white_player_id'] == 'hikaru' and row['white_rating'] > row['black_rating'] else 0,
    axis=1
)

df_hikaru['win_black'] = df_hikaru.apply(
    lambda row: 1 if row['black_player_id'] == 'hikaru' and row['black_rating'] > row['white_rating'] else 0,
    axis=1
)

# Overall win rate for Hikaru across all games
overall_win_rate = (df_hikaru['win_white'].sum() + df_hikaru['win_black'].sum()) / len(df_hikaru)
print(f"Hikaru's overall win rate: {overall_win_rate * 100:.2f}%")

# 1. Time Control Distribution (Rating vs Time Control)
plt.figure(figsize=(10, 6))
sns.boxplot(data=df_hikaru, x='time_control', y='white_rating') # corrected boxplot.
plt.title("Hikaru's Rating Distribution by Time Control")
plt.xlabel("Time Control")
plt.ylabel("Rating")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 3. Opponent Ratings Comparison - Heatmap of Wins vs Opponent Ratings
df_hikaru['opponent_rating'] = np.where(df_hikaru['white_player_id'] == 'hikaru', df_hikaru['black_rating'], df_hikaru['white_rating'])
df_hikaru['win'] = np.where(df_hikaru['win_white'] == 1, 1, 0)
df_hikaru['win'] = np.where(df_hikaru['win_black'] == 1, 1, df_hikaru['win'])

# Group by opponent ratings and calculate the win rate
opponent_rating_win_rate = df_hikaru.groupby('opponent_rating')['win'].mean().reset_index()

# Create a heatmap of opponent ratings vs win rate
heatmap_data = opponent_rating_win_rate.pivot_table(values='win', index='opponent_rating', columns='win', aggfunc='mean')

# Check if heatmap_data is empty before plotting
if not heatmap_data.empty:
    plt.figure(figsize=(10, 6))
    sns.heatmap(heatmap_data, annot=True, cmap='Blues', fmt='.2f', linewidths=0.5)
    plt.title("Opponent Rating vs Win Rate Heatmap")
    plt.xlabel("Win Rate")
    plt.ylabel("Opponent Rating")
    plt.tight_layout()
    plt.show()
else:
    print("Heatmap data is empty. Skipping heatmap plot.")

# # 4. Performance by Opening
# # We need to extract the opening from the PGN data (if available). Let's assume the PGN contains opening information.

# def extract_opening(pgn):
#     """ Extract the opening name from the PGN, if available. """
#     opening_match = re.search(r'\[Opening "(.+?)"\]', pgn)
#     return opening_match.group(1) if opening_match else "Unknown"

# df_hikaru['opening'] = df_hikaru['pgn'].apply(extract_opening)

# # Add a column for win or loss based on the game result
# df_hikaru['result'] = df_hikaru.apply(
#     lambda row: 'win' if row['win_white'] == 1 or row['win_black'] == 1 else 'loss', axis=1
# )

# # Group by opening and calculate win rate for each opening
# opening_win_rate = df_hikaru.groupby('opening')['result'].apply(lambda x: (x == 'win').mean()).reset_index()

# # Sort the openings by win rate
# opening_win_rate_sorted = opening_win_rate.sort_values(by='result', ascending=False)

# # Plot the win rate by opening
# plt.figure(figsize=(12, 8))
# sns.barplot(data=opening_win_rate_sorted, x='opening', y='result', palette='viridis')
# plt.title("Hikaru's Performance by Opening")
# plt.xlabel("Opening")
# plt.ylabel("Win Rate")
# plt.xticks(rotation=90)
# plt.tight_layout()
# plt.show()
