import pandas as pd
import requests
import re
import os
import json
import redis
import time
import shutil
import datetime
from os.path import exists
from os.path import join
# from constants import data_dir, swagger_data_dir, swagger_player_gamelogs_dir, swagger_season_dir, swagger_gamestats_dir, swagger_gameflow_dir, data_dir_dk, nba_season, dk_to_nba_team_id

def salary_timezone_data(salary_df):
    game_info_split = salary_df['gameinfo'].str.split(' ', expand=True)
    date = game_info_split[1]
    time_et = game_info_split[2].str.split(' ', expand=True)[0]
    salary_df['datetime'] = date + ' ' + time_et
    salary_df['datetime'] = pd.to_datetime(
        salary_df['datetime'],
        format='%m/%d/%Y %I:%M%p').dt.tz_localize('US/Eastern')
    # salary_df['datetime_mtn'] = salary_df['datetime'].dt.tz_convert(
    #     'US/Mountain')
    salary_df['game_date'] = salary_df['datetime_mtn'].dt.strftime('%Y-%m-%d')
    salary_df['time'] = salary_df['datetime_mtn'].dt.strftime('%I:%M %p')



rd = redis.Redis(host='redis-15539.c331.us-west1-1.gce.redns.redis-cloud.com',
                 port=15539,
                 password='5p4mGkXnAklU5JcwbHoVrMg1c8U8IktY',
                 decode_responses=True)

slateID = "Turbo1"

dksal_path = f"sal-{slateID}.csv"
dksal_df = pd.read_csv(dksal_path,
                       usecols=[
                           'Position', 'Name + ID', 'Name', 'ID',
                           'Roster Position', 'Salary', 'Game Info',
                           'TeamAbbrev', 'AvgPointsPerGame'
                       ])
dksal_df.dropna(inplace=True)
dksal_df.columns = dksal_df.columns.str.lower().str.replace(" ", "")
