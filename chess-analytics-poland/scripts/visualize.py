import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def extract_year_month(start_time):
    """Extract year and month from start_time (Unix timestamp)."""
    try:
        # Convert Unix timestamp to datetime object
        start_datetime = pd.to_datetime(start_time, unit='s')

        # Extract year and month as a period (e.g., 2014-03)
        year_month = start_datetime.to_period('M')
        return year_month
    except Exception as e:
        print(f"Error parsing start_time: {start_time} - {e}")
        return None  # Return None if there's an issue

# Example usage with your data:
game_data = [
    {
        "start_time": 1395168295,  # Example Unix timestamp
        "winner": "Hikaru",
        "time_class": "daily",
        "rating_white": 2361,
        "rating_black": 1476
    },
    {
        "start_time": 1395169395,  # Another example timestamp
        "winner": "15ATP",
        "time_class": "daily",
        "rating_white": 2400,
        "rating_black": 1500
    },
    # Add more game data here...
]

# Convert the data into a DataFrame
df = pd.DataFrame(game_data)

# Apply the extraction function to the 'start_time' field
df['year_month'] = df['start_time'].apply(extract_year_month)

# Drop rows with invalid dates
df = df.dropna(subset=['year_month'])

# Count the number of games per month/year
games_by_month = df['year_month'].value_counts().sort_index()

# Plotting the data
plt.figure(figsize=(12, 6))
sns.barplot(x=games_by_month.index.astype(str), y=games_by_month.values)
plt.title('Games Distribution by Month/Year')
plt.xlabel('Month/Year')
plt.ylabel('Number of Games')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
