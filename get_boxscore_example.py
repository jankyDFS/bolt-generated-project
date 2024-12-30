import pandas as pd
from nba_api.live.nba.endpoints import boxscore

# Define the game ID
GAME_ID = '0022400391'

# Get the boxscore data
box = boxscore.BoxScore(GAME_ID)

# Print outputs for various data sets

# Full game data
game_data = box.game.get_dict()
print("Game Data:", game_data)
pd.DataFrame(game_data)

# Arena data
arena_data = box.arena.get_dict()
print("Arena Data:", arena_data)

# Away team data
away_team_data = box.away_team.get_dict()
print("Away Team Data:", away_team_data)

# Away team player stats
away_team_player_stats = box.away_team_player_stats.get_dict()
print("Away Team Player Stats:", away_team_player_stats)

# Away team aggregated stats
away_team_stats = box.away_team_stats.get_dict()
print("Away Team Stats:", away_team_stats)

# Home team data
home_team_data = box.home_team.get_dict()
print("Home Team Data:", home_team_data)

# Home team player stats
home_team_player_stats = box.home_team_player_stats.get_dict()
print("Home Team Player Stats:", home_team_player_stats)

# Home team aggregated stats
home_team_stats = box.home_team_stats.get_dict()
print("Home Team Stats:", home_team_stats)

# Game details
game_details = box.game_details.get_dict()
print("Game Details:", game_details)

# Officials data
officials_data = box.officials.get_dict()
print("Officials Data:", officials_data)


import pickle

# Save the object as a pickle file
with open("box_response.pkl", "wb") as file:
    pickle.dump(box, file)

with open("box_response.pkl", "rb") as file:
    loaded_box = pickle.load(file)


# Full game data
loaded_game_data = loaded_box.get_dict()
print("Game Data:", loaded_game_data)

officials_data = loaded_box.officials.get_dict()
print("Officials Data:", officials_data)
