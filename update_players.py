import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')

def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            dbname=DB_NAME
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise

def create_player_ids_table(conn):
    cur = conn.cursor()
    try:
        create_sql = '''
            CREATE TABLE IF NOT EXISTS player_ids (
                id INT PRIMARY KEY,
                full_name TEXT NOT NULL,
                is_active BOOLEAN NOT NULL
            )
        '''

        cur.execute(create_sql)
        conn.commit()
        print("Table 'player_ids' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cur.close()

def append_new_player_ids(conn, df):
    try:
        for index, row in df.iterrows():
            insert_sql = '''
                INSERT INTO player_ids (id, full_name, is_active)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET is_active = EXCLUDED.is_active;
            '''

            conn.cursor().execute(insert_sql, (
                row['id'], row['full_name'], row['is_active']
            ))

        conn.commit()
        print("Player IDs inserted/updated successfully in the 'player_ids' table.")
    except Exception as e:
        print(f"Error inserting/updating player IDs: {e}")

if __name__ == '__main__':
    # Step 1: Create the player_ids table if it doesn't exist
    conn = connect_to_postgres()
    create_player_ids_table(conn)

    # Step 2: Append new player IDs to the table
    df_players = pd.DataFrame({
        'id': [1, 2, 3],
        'full_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'is_active': [True, False, True]
    })

    append_new_player_ids(conn, df_players)
