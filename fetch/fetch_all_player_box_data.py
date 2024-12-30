import psycopg2
import pandas as pd

# Database connection details
DB_HOST = 'localhost'
DB_USER = 'db_user'
DB_PASS = 'hello'
DB_NAME = 'nba_api'
OUTPUT_CSV_FILE = "player_box_data.csv"  # Path to save the CSV file


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


# Function to retrieve all data from player_box table
def fetch_player_box_data():
    conn = connect_to_postgres()
    try:
        query = "SELECT * FROM player_box;"
        # Use pandas to execute the query and fetch data
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"Error retrieving data from player_box: {e}")
    finally:
        conn.close()


# Main function to fetch data and save as CSV
def save_player_box_to_csv():
    print("Fetching data from player_box table...")
    df = fetch_player_box_data()

    if df is not None and not df.empty:
        # Save DataFrame to a CSV file
        df.to_csv(OUTPUT_CSV_FILE, index=False)
        print(f"Data successfully saved to {OUTPUT_CSV_FILE}")
    else:
        print("No data found in player_box table or an error occurred.")


# Execute the main function
if __name__ == "__main__":
    save_player_box_to_csv()
