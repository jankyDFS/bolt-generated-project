import os
import time
from datetime import datetime
import pandas as pd
import psycopg2
from nba_api.live.nba.endpoints import boxscore
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

BOX_FOLDER = "data/nba_api/box"

# Function to connect to PostgreSQL
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

# Function to get team IDs
def get_team_ids():
    conn = connect_to_postgres()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM teams_api;")
        team_ids = [row[0] for row in cur.fetchall()]
        return team_ids
    except Exception as e:
        print(f"Error retrieving team IDs: {e}")
    finally:
        cur.close()
        conn.close()

# Function to get filtered game IDs
def get_filtered_games(team_ids):
    """
    Retrieves a list of game IDs for specified team IDs, excluding games that already exist in the player_box table.

    Args:
        team_ids (list[int]): List of team IDs to filter games by.

    Returns:
        list[str]: List of filtered game IDs.
    """
    conn = connect_to_postgres()
    cur = conn.cursor()
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        query = """
            SELECT DISTINCT g.game_id, g.game_date
            FROM games g
            WHERE LEFT(g.season_id, 1) NOT IN ('1', '3', '5')
            AND g.team_id = ANY(%s::int[])
            AND g.game_date != %s
            AND NOT EXISTS (
                SELECT 1
                FROM player_box pb
                WHERE pb.game_id = g.game_id
            )
            ORDER BY g.game_date ASC;
        """
        cur.execute(query, (team_ids, today))
        game_ids: list[str] = [str(row[0]) for row in cur.fetchall()]
        return game_ids
    except Exception as e:
        print(f"Error retrieving games: {e}")
        return []
    finally:
        cur.close()
        conn.close()



# Function to add double-double and triple-double stats
def add_doubles(df):
    df['DD'] = ((df[['points', 'reboundsTotal', 'assists', 'steals', 'blocks']] >= 10).sum(axis=1) >= 2).astype(bool)
    df['TD'] = ((df[['points', 'reboundsTotal', 'assists', 'steals', 'blocks']] >= 10).sum(axis=1) >= 3).astype(bool)

# Function to calculate fantasy points
def calculate_FPTS(df):
    return (
        df['points'] +
        df['threePointersMade'] * 0.5 +
        df['reboundsTotal'] * 1.25 +
        df['assists'] * 1.5 +
        df['steals'] * 2 +
        df['blocks'] * 2 -
        df['turnovers'] * 0.5 +
        df['DD'] * 1.5 +
        df['TD'] * 3
    )

# Function to prepare the DataFrame for SQL insertion
def prepare_dataframe_for_sql(df):
    df = df.where(pd.notna(df), None)  # Replace NaN with None

    # Cast integer columns to float where NaN is possible
    int_cols_with_nan = [
        'order', 'assists', 'blocks', 'blocksReceived', 'fieldGoalsAttempted',
        'fieldGoalsMade', 'foulsOffensive', 'foulsDrawn', 'foulsPersonal',
        'foulsTechnical', 'freeThrowsAttempted', 'freeThrowsMade',
        'reboundsDefensive', 'reboundsOffensive', 'reboundsTotal', 'steals',
        'threePointersAttempted', 'threePointersMade', 'turnovers',
        'twoPointersAttempted', 'twoPointersMade', 'pointsFastBreak',
        'pointsInThePaint', 'pointsSecondChance', 'points'
    ]
    for col in int_cols_with_nan:
        if col in df.columns:
            df[col] = df[col].astype('float64', errors='ignore')

    # Handle boolean columns
    bool_cols = ['starter', 'oncourt', 'played']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].map({'1': True, '0': False, None: None})

    return df

# Function to fetch and save boxscore data
def fetch_and_save_boxscore(game_id):
    try:
        print(f"Fetching boxscore for game_id: {game_id}")
        box = boxscore.BoxScore(str(game_id))
        game_stats_dict = box.get_dict()

        # Load player data
        home_players = game_stats_dict['game']['homeTeam']['players']
        away_players = game_stats_dict['game']['awayTeam']['players']

        home_df = pd.DataFrame(home_players)
        away_df = pd.DataFrame(away_players)

        # Flatten statistics
        if 'statistics' in home_df.columns:
            home_stats = pd.json_normalize(home_df['statistics'])
            home_df = pd.concat([home_df.drop(columns=['statistics']), home_stats], axis=1)

        if 'statistics' in away_df.columns:
            away_stats = pd.json_normalize(away_df['statistics'])
            away_df = pd.concat([away_df.drop(columns=['statistics']), away_stats], axis=1)

        # Add team and game_id columns
        home_df['team'] = game_stats_dict['game']['homeTeam']['teamName']
        home_df['game_id'] = game_id

        away_df['team'] = game_stats_dict['game']['awayTeam']['teamName']
        away_df['game_id'] = game_id

        # Combine home and away DataFrames
        combined_stats_df = pd.concat([away_df, home_df], ignore_index=True)

        # Add doubles and FPTS
        add_doubles(combined_stats_df)
        combined_stats_df['FPTS'] = calculate_FPTS(combined_stats_df)

        # Prepare DataFrame for SQL
        combined_stats_df = prepare_dataframe_for_sql(combined_stats_df)

        # Connect to database and insert data
        conn = connect_to_postgres()
        cur = conn.cursor()

        # Ensure table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_box (
                game_id TEXT NOT NULL,
                personId INT NOT NULL,
                status TEXT,
                order_num INT,
                jerseyNum TEXT,
                position TEXT,
                starter BOOLEAN,
                oncourt BOOLEAN,
                played BOOLEAN,
                name TEXT,
                assists INT,
                blocks INT,
                blocksReceived INT,
                fieldGoalsAttempted INT,
                fieldGoalsMade INT,
                fieldGoalsPercentage FLOAT,
                foulsOffensive INT,
                foulsDrawn INT,
                foulsPersonal INT,
                foulsTechnical INT,
                freeThrowsAttempted INT,
                freeThrowsMade INT,
                freeThrowsPercentage FLOAT,
                minus FLOAT,
                minutes TEXT,
                minutesCalculated TEXT,
                plus FLOAT,
                plusMinusPoints FLOAT,
                points INT,
                pointsFastBreak INT,
                pointsInThePaint INT,
                pointsSecondChance INT,
                reboundsDefensive INT,
                reboundsOffensive INT,
                reboundsTotal INT,
                steals INT,
                threePointersAttempted INT,
                threePointersMade INT,
                threePointersPercentage FLOAT,
                turnovers INT,
                twoPointersAttempted INT,
                twoPointersMade INT,
                twoPointersPercentage FLOAT,
                DD BOOLEAN,
                TD BOOLEAN,
                FPTS FLOAT,
                team TEXT,
                PRIMARY KEY (game_id, personId)
            );
        """)

        # Insert new records
        for _, row in combined_stats_df.iterrows():
            try:
                sql = """
                    INSERT INTO player_box VALUES (
                        %(game_id)s, %(personId)s, %(status)s, %(order)s, %(jerseyNum)s,
                        %(position)s, %(starter)s, %(oncourt)s, %(played)s, %(name)s,
                        %(assists)s, %(blocks)s, %(blocksReceived)s, %(fieldGoalsAttempted)s,
                        %(fieldGoalsMade)s, %(fieldGoalsPercentage)s, %(foulsOffensive)s,
                        %(foulsDrawn)s, %(foulsPersonal)s, %(foulsTechnical)s,
                        %(freeThrowsAttempted)s, %(freeThrowsMade)s, %(freeThrowsPercentage)s,
                        %(minus)s, %(minutes)s, %(minutesCalculated)s, %(plus)s,
                        %(plusMinusPoints)s, %(points)s, %(pointsFastBreak)s,
                        %(pointsInThePaint)s, %(pointsSecondChance)s, %(reboundsDefensive)s,
                        %(reboundsOffensive)s, %(reboundsTotal)s, %(steals)s,
                        %(threePointersAttempted)s, %(threePointersMade)s,
                        %(threePointersPercentage)s, %(turnovers)s, %(twoPointersAttempted)s,
                        %(twoPointersMade)s, %(twoPointersPercentage)s, %(DD)s, %(TD)s,
                        %(FPTS)s, %(team)s
                    )
                    ON CONFLICT (game_id, personId) DO NOTHING;
                """
                cur.execute(sql, row.to_dict())
            except Exception as e:
                print(f"Error inserting row: {row.to_dict()}")
                print(f"Exception: {e}")

        conn.commit()
        cur.close()
        conn.close()
        print(f"Boxscore for game {game_id} processed successfully.")
        return True
    except Exception as e:
        print(f"Error fetching or saving boxscore for game {game_id}: {e}")
        return False

# Main Execution
if __name__ == '__main__':
    os.makedirs(BOX_FOLDER, exist_ok=True)

    team_ids = get_team_ids()
    print(f"Loaded {len(team_ids)} team IDs.")

    game_ids = get_filtered_games(team_ids)
    print(f"Found {len(game_ids)} unique game IDs to process.")

    for game_id in game_ids:
        should_delay = fetch_and_save_boxscore(game_id)
        if should_delay:
            time.sleep(5)
