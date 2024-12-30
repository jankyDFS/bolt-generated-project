from nba_api.stats.endpoints import leaguegamefinder
import pandas as pd
import psycopg2
from datetime import datetime
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


valid_abbreviations = [
    'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN',
    'DET', 'GSW', 'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA',
    'MIL', 'MIN', 'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHX',
    'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
]


season = '2024-25'

# Fetch games data
gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
games = gamefinder.get_data_frames()[0]

# Filter games based on SEASON_ID
print(games['SEASON_ID'].unique())

# Drop rows with today's date
today = datetime.now().strftime('%Y-%m-%d')
games_to_store = games[games['GAME_DATE'] != today]
games_to_store = games_to_store[games_to_store['TEAM_ABBREVIATION'].isin(valid_abbreviations)]


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


def create_game_ids_table():
    """
    Creates the 'game_ids' table if it does not exist.
    """
    conn = connect_to_postgres()
    cur = conn.cursor()

    try:
        create_sql = '''
            CREATE TABLE IF NOT EXISTS game_ids (
                game_id TEXT PRIMARY KEY,
                game_date TEXT NOT NULL
            )
        '''
        cur.execute(create_sql)
        conn.commit()
        print("Table 'game_ids' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cur.close()
        conn.close()


def append_new_game_ids(df):
    """
    Appends unique game_id and game_date pairs to the 'game_ids' table.

    Parameters:
        df (pd.DataFrame): The filtered games DataFrame.
    """
    conn = connect_to_postgres()
    cur = conn.cursor()

    try:
        unique_games = df[['GAME_ID', 'GAME_DATE']].drop_duplicates()
        for index, row in unique_games.iterrows():
            insert_sql = '''
                INSERT INTO game_ids (game_id, game_date)
                VALUES (%s, %s)
                ON CONFLICT (game_id) DO NOTHING;
            '''
            cur.execute(insert_sql, (row['GAME_ID'], row['GAME_DATE']))

        conn.commit()
        print("New game IDs inserted successfully into the 'game_ids' table.")
    except Exception as e:
        print(f"Error inserting game IDs: {e}")
    finally:
        cur.close()
        conn.close()


def create_games_table():
    conn = connect_to_postgres()
    cur = conn.cursor()

    try:
        create_sql = '''
            CREATE TABLE IF NOT EXISTS games (
                season_id TEXT NOT NULL,
                team_id INT NOT NULL,
                team_abbreviation TEXT NOT NULL,
                team_name TEXT NOT NULL,
                game_id TEXT NOT NULL,
                game_date TEXT NOT NULL,
                matchup TEXT NOT NULL,
                wl TEXT,
                min TEXT,
                pts INT,
                fgm INT,
                fga INT,
                fg_pct FLOAT,
                fg3m INT,
                fg3a INT,
                fg3_pct FLOAT,
                ftm INT,
                fta INT,
                ft_pct FLOAT,
                oreb INT,
                dreb INT,
                reb INT,
                ast INT,
                stl INT,
                blk INT,
                tov INT,
                pf INT,
                plus_minus FLOAT,
                PRIMARY KEY (game_id, team_id)
            )
        '''
        cur.execute(create_sql)
        conn.commit()
        print("Table 'games' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cur.close()
        conn.close()


def append_new_games(df):
    conn = connect_to_postgres()
    cur = conn.cursor()

    try:
        for index, row in df.iterrows():
            insert_sql = '''
                INSERT INTO games_sched (season_id, team_id, team_abbreviation, team_name, game_id, game_date, matchup, wl, min, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, tov, pf, plus_minus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_id, team_id) DO NOTHING;
            '''
            cur.execute(insert_sql, (
                row['SEASON_ID'], row['TEAM_ID'], row['TEAM_ABBREVIATION'], row['TEAM_NAME'],
                row['GAME_ID'], row['GAME_DATE'], row['MATCHUP'], row['WL'], row['MIN'],
                row['PTS'], row['FGM'], row['FGA'], row['FG_PCT'], row['FG3M'], row['FG3A'],
                row['FG3_PCT'], row['FTM'], row['FTA'], row['FT_PCT'], row['OREB'],
                row['DREB'], row['REB'], row['AST'], row['STL'], row['BLK'], row['TOV'],
                row['PF'], row['PLUS_MINUS']
            ))

        conn.commit()
        print("New games data inserted successfully into the 'games' table.")
    except Exception as e:
        print(f"Error inserting game data: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    # Step 1: Create the game_ids table if it doesn't exist
    create_game_ids_table()

    # Step 2: Append new game IDs to the table
    append_new_game_ids(games_to_store)

    # Step 3: Create the games table if it doesn't exist
    create_games_table()

    # Step 4: Append new games data to the table
    append_new_games(games_to_store)
