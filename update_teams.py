from nba_api.stats.static import teams
import pandas as pd
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

# Get teams data
teams_data = teams.get_teams()
print("Number of teams fetched: {}".format(len(teams_data)))

# Convert the team data into a DataFrame for easier handling
df_teams = pd.DataFrame(teams_data)

def connect_to_postgres():
    """
    Connects to the PostgreSQL server and specified database.

    Returns:
        conn: psycopg2 connection object.
    """
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

def create_teams_table():
    """
    Creates the 'teams' table if it does not exist.
    """
    conn = connect_to_postgres()
    cur = conn.cursor()

    try:
        create_sql = '''
            CREATE TABLE IF NOT EXISTS teams (
                id INT PRIMARY KEY,
                full_name TEXT NOT NULL,
                abbreviation TEXT NOT NULL,
                nickname TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT NOT NULL,
                year_founded INT NOT NULL
            )
        '''

        cur.execute(create_sql)
        conn.commit()
        print("Table 'teams' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cur.close()
        conn.close()

def append_new_teams(df):
    """
    Appends new team data to the 'teams' table.

    Parameters:
        df (pd.DataFrame): A DataFrame containing the teams data.
    """
    conn = connect_to_postgres()
    cur = conn.cursor()

    try:
        for index, row in df.iterrows():
            insert_sql = '''
                INSERT INTO teams (id, full_name, abbreviation, nickname, city, state, year_founded)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            '''

            cur.execute(insert_sql, (
                row['id'], row['full_name'], row['abbreviation'], row['nickname'],
                row['city'], row['state'], row['year_founded']
            ))

        conn.commit()
        print("New teams data inserted successfully into the 'teams' table.")
    except Exception as e:
        print(f"Error inserting team data: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    # Step 1: Create the teams table if it doesn't exist
    create_teams_table()

    # Step 2: Append new teams data to the table
    append_new_teams(df_teams)
