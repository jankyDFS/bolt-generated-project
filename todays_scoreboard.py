from datetime import timezone, datetime
from dateutil import parser
from nba_api.live.nba.endpoints import scoreboard
import json
import psycopg2
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# Access the variables
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')

print(f"Database Host: {DB_HOST}")

f = "{gameId}: {awayTeam} vs. {homeTeam} @ {gameTimeLTZ}"

board = scoreboard.ScoreBoard()

with open("scoreboard_raw.json", "w") as file:
    json.dump(board.get_dict(), file, indent=4)

print("ScoreBoardDate: " + board.score_board_date)

games = board.games.get_dict()

with open("games_raw.json", "w") as file:
    json.dump(games, file, indent=4)

def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        exit(1)

connection = connect_to_postgres()
cursor = connection.cursor()

cursor.execute("DROP TABLE IF EXISTS games_today;")
cursor.execute("""
CREATE TABLE games_today (
    game_id VARCHAR(255) PRIMARY KEY,
    away VARCHAR(255) REFERENCES teams(team_nickname),
    home VARCHAR(255) REFERENCES teams(team_nickname),
    datetime TIMESTAMP
);
""")

insert_query = "INSERT INTO games_today (game_id, away, home, datetime) VALUES (%s, %s, %s, %s);"
for game in games:
    game_id = game['gameId']
    away_team = game['awayTeam']['teamName']
    home_team = game['homeTeam']['teamName']
    game_datetime = parser.parse(game["gameTimeUTC"]).replace(tzinfo=timezone.utc).astimezone(tz=None)
    cursor.execute(insert_query, (game_id, away_team, home_team, game_datetime))

connection.commit()
cursor.close()
connection.close()

for game in games:
    gameTimeLTZ = parser.parse(game["gameTimeUTC"]).replace(tzinfo=timezone.utc).astimezone(tz=None)
    formattedTime = gameTimeLTZ.strftime("%Y-%m-%d %I:%M %p")
    print(f.format(gameId=game['gameId'], awayTeam=game['awayTeam']['teamName'], homeTeam=game['homeTeam']['teamName'], gameTimeLTZ=formattedTime))



# for game in games:
#     gameTimeLTZ = parser.parse(game["gameTimeUTC"]).replace(tzinfo=timezone.utc).astimezone(tz=None)
#     formattedTime = gameTimeLTZ.strftime("%Y-%m-%d %I:%M %p")
#     print(f.format(gameId=game['gameId'], awayTeam=game['awayTeam']['teamName'], homeTeam=game['homeTeam']['teamName'], gameTimeLTZ=formattedTime))
