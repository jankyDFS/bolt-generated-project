import numpy as np
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import redis
from typing import List
import psycopg2
import re

app = FastAPI()

# Redis connection
rd = redis.Redis(
    host='redis-15539.c331.us-west1-1.gce.redns.redis-cloud.com',
    port=15539,
    password='5p4mGkXnAklU5JcwbHoVrMg1c8U8IktY',
    decode_responses=True
)

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for specific domains in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



# Function to connect to PostgreSQL
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            host='localhost',
            user='db_user',
            password='hello',
            dbname='nba_api'
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        exit(1)


# Helper function to process the CSV file
def process_salary_file(slate_id: str):
    try:
        # Read the file
        salary_df = pd.read_csv(f"sal-{slate_id}.csv")

        # Process the DataFrame
        game_info_split = salary_df['Game Info'].str.split(' ', expand=True)
        date = game_info_split[1]
        time_et = game_info_split[2].str.split(' ', expand=True)[0]
        salary_df['datetime'] = date + ' ' + time_et
        salary_df['datetime'] = pd.to_datetime(
            salary_df['datetime'],
            format='%m/%d/%Y %I:%M%p'
        ).dt.tz_localize('US/Eastern')

        unique_teams = sorted(salary_df['TeamAbbrev'].unique())
        redis_key = f"react_slate_teams_{slate_id}"
        rd.set(redis_key, ",".join(unique_teams))

        def get_opponent(row):
            teams = row['Game Info'].split(' ')[0].split('@')
            return teams[0] if row['TeamAbbrev'] == teams[1] else teams[1]

        salary_df['opp'] = salary_df.apply(get_opponent, axis=1)

        conn = connect_to_postgres()
        cursor = conn.cursor()

        def get_team_id(team_abbrev):
            query = "SELECT nba_team_id FROM teams WHERE abv = %s"
            cursor.execute(query, (team_abbrev,))
            result = cursor.fetchone()
            return result[0] if result else None

        salary_df['team_id'] = salary_df['TeamAbbrev'].apply(get_team_id)
        salary_df['opp_team_id'] = salary_df['opp'].apply(get_team_id)

        name_corrections = {
            "jakobpoltl": "jakobpoeltl",
            "ggjackson": "gregoryjackson"
        }

        def create_shortname(name: str) -> str:
            name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
            for suffix in [" Jr", " III", " II", " IV", " Sr"]:
                if name.endswith(suffix):
                    name = name[: -len(suffix)]
            return name.replace(" ", "").lower()

        salary_df['shortname'] = salary_df['Name'].apply(create_shortname)

        cursor.execute("SELECT id, unaccent(full_name) AS normalized_name FROM player_ids")
        player_ids = cursor.fetchall()

        player_ids_df = pd.DataFrame(player_ids, columns=['id', 'full_name'])
        player_ids_df['shortname'] = player_ids_df['full_name'].apply(create_shortname)

        player_ids_dict = player_ids_df.set_index('shortname')['id'].to_dict()
        salary_df['player_id'] = salary_df['shortname'].map(player_ids_dict).apply(lambda x: int(x) if pd.notnull(x) else '')

        unmatched_names = salary_df[salary_df['player_id'].isna()]['Name'].tolist()
        if unmatched_names:
            print("Unmatched Names:", unmatched_names)

        conn.close()

        salary_df['game_date'] = salary_df['datetime'].dt.strftime('%Y-%m-%d')
        salary_df['slateID'] = slate_id
        salary_df.columns = salary_df.columns.str.lower().str.replace(" ", "").str.replace("+", "")

        # Save the processed DataFrame
        file_path = f"sal-{slate_id}-processed.csv"
        salary_df.to_csv(file_path, index=False)
        print(salary_df)

        # Insert into database
        conn = connect_to_postgres()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        columns = salary_df.columns
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS dksal (
            {", ".join(f"{col} TEXT" for col in columns)},
            UNIQUE (player_id, game_date, slateid)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Insert unique records
        for _, row in salary_df.iterrows():
            columns_list = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"""
            INSERT INTO dksal ({columns_list})
            VALUES ({placeholders})
            ON CONFLICT (player_id, game_date, slateid) DO NOTHING;
            """
            cursor.execute(insert_query, tuple(row))

        conn.commit()
        conn.close()

        sal_file_date = salary_df['game_date'].max()

        return {
            "status": "success",
            "max_game_date": sal_file_date,
            "processed_rows": len(salary_df),
            "unique_teams_key": redis_key
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}




# Endpoint to process slate IDs
@app.post("/process-slates/")
async def process_slates(slates: List[str]):
    results = {}
    redis_key = "react_slateIDs_today"
    rd.set(redis_key, ",".join(slates))
    for slate_id in slates:
        # Construct the file path for the slate
        # file_path = f"sal-{slate_id}.csv"
        result = process_salary_file(slate_id)
        results[slate_id] = result

    return results


@app.get("/get-slate-ids/")
async def get_slate_ids():
    try:
        slate_ids = rd.get("react_slateIDs_today")
        if not slate_ids:
            return {"status": "error", "message": "No slate IDs found in Redis"}
        slate_ids_list = slate_ids.split(",")  # Convert the comma-separated string to a list
        return {"status": "success", "slate_ids": slate_ids_list}
    except Exception as e:
        return {"status": "error", "message": str(e)}


import os
import pandas as pd
from fastapi.responses import JSONResponse

@app.get("/get-slate-data/{slate_id}")
async def get_slate_data(slate_id: str):
    try:
        file_path = f"sal-{slate_id}-processed.csv"
        if not os.path.exists(file_path):
            return JSONResponse(
                status_code=404,
                content={"status": "error", "message": f"File {file_path} not found"}
            )
        # Load the CSV file
        df = pd.read_csv(file_path)

        # Replace NaN values with an appropriate placeholder
        df = df.fillna("")  # Replace NaN with an empty string for JSON compatibility

        # Convert to JSON format
        data = df.to_dict(orient="records")

        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/get-updated-data/{slate_id}")
async def get_updated_data(slate_id: str):
    try:
        # Define the path to the CSV containing updated data
        updated_file_path = f"awesemo_proj.csv"
        if not os.path.exists(updated_file_path):
            return JSONResponse(
                status_code=404,
                content={"status": "error", "message": f"File {updated_file_path} not found"}
            )

        # Load the CSV file
        awesemo_proj = pd.read_csv(updated_file_path)
        remove_words = [" Jr", " III", " II", " IV", " Sr"]
        pat = "|".join(remove_words)
        awesemo_proj['shortname'] = awesemo_proj['Name'].str.replace(pat, "", regex=True).str.lower()
        awesemo_proj.drop_duplicates(subset="shortname", keep='first', inplace=True)
        # awesemo_proj['FPM'] = (awesemo_proj['Fpts'] / awesemo_proj['Minutes']).round(3)
        awesemo_proj.rename(columns={"Fpts": "proj", "Minutes": "min"}, inplace=True)

        # Ensure the CSV contains 'shortname', 'proj', and 'min' columns
        if not {'shortname', 'proj', 'min'}.issubset(awesemo_proj.columns):
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "CSV missing required columns"}
            )

        # Convert the data to a list of dictionaries
        awesemo_updated_data = awesemo_proj[['shortname', 'proj', 'min']].fillna("").to_dict(orient="records")
        print(awesemo_updated_data)


        return {"status": "success", "data": awesemo_updated_data}
    except Exception as e:
        return {"status": "error", "message": str(e)}
