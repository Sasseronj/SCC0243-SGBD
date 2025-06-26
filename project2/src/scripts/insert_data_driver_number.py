import sys
import pandas as pd
import os
from tqdm import tqdm
import time

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
from dotenv import dotenv_values
from sqlalchemy import create_engine
from joblib import Parallel, delayed

token = dotenv_values(".env.local")['INFLUXDB_TOKEN']
org = "my-org"
bucket = "bucket-driver-number"
url = "http://localhost:8086"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

def insert_tyre_strints():
    df_tyre_strints = pd.read_parquet("./data/stints/stints.parquet").dropna()
    df_tyre_strints = df_tyre_strints.drop(columns=["meeting_key"])
    df_tyre_strints = df_tyre_strints.drop_duplicates(subset=["session_key", "stint_number", "driver_number"])

    for _, row in tqdm(df_tyre_strints.iterrows(), total=len(df_tyre_strints)):
        try:
            point = (
                Point("tyre_stints")
                .tag("session_key", str(row["session_key"]))
                .tag("driver_number", str(row["driver_number"]))
                .tag("compound", str(row["compound"]))
                .field("stint_number", int(row["stint_number"]))
                .field("tyre_age_at_start", float(row["tyre_age_at_start"]))
                .field("lap_start", int(row["lap_start"]))
                .field("lap_end", int(row["lap_end"]))
                .time(pd.Timestamp.utcnow(), WritePrecision.NS)
            )
            write_api.write(bucket=bucket, org=org, record=point)
        except Exception as e:
            print(f"Error processing row: {e}")
            
def insert_weather_conditions():
    df_weather = pd.read_parquet("./data/weather_conditions/weather_conditions.parquet").dropna()
    df_session = pd.read_parquet("./data/sessions/sessions.parquet")
    df_weather = df_weather.drop(columns=["meeting_key"])
    df_weather = df_weather.drop_duplicates(subset=["session_key", "date"])
    df_weather = df_weather[df_weather["session_key"].isin(df_session["session_key"].to_list())]
    df_weather["rainfall"] = df_weather["rainfall"].astype(bool)
    
    for _, row in tqdm(df_weather.iterrows(), total=len(df_weather)):
        point = (
            Point("weather_conditions")
            .tag("session_key", str(row["session_key"]))
            .field("track_temperature", float(row["track_temperature"]))
            .field("wind_speed", float(row["wind_speed"]))
            .field("rainfall", int(row["rainfall"]))  
            .field("humidity", float(row["humidity"]))
            .field("pressure", float(row["pressure"]))
            .field("air_temperature", float(row["air_temperature"]))
            .field("wind_direction", float(row["wind_direction"])) 
            .time(row["date"], WritePrecision.NS)
        )
        write_api.write(bucket=bucket, org=org, record=point)

def insert_meets():
    # Meets
    df_meets = pd.read_parquet("./data/meetings/meetings.parquet").dropna()
    df_meets["meeting_key"] = df_meets["meeting_key"].astype(str)
    df_meets["date_start"] = pd.to_datetime(df_meets["date_start"])

    for _, row in tqdm(df_meets.iterrows(), total=len(df_meets)):
        p = (
            Point("meetings")
            .tag("meeting_key", str(row["meeting_key"]))
            .tag("country_code", str(row["country_code"]))
            .tag("circuit_short_name", str(row["circuit_short_name"]))
            .tag("meeting_name", str(row["meeting_name"]))
            .tag("meeting_official_name", str(row["meeting_official_name"]))
            .tag("location", str(row["location"]))
            .tag("country_key", int(row["country_key"]))
            .tag("country_name", str(row["country_name"]))
            .field("circuit_key", int(row["circuit_key"]))
            .field("gmt_offset", str(row["gmt_offset"]))
            .field("year", int(row["year"]))
            .time(row["date_start"], WritePrecision.NS)
        )
        write_api.write(bucket=bucket, org=org, record=p)

def insert_sessions():
    df_sessions = pd.read_parquet("./data/sessions/sessions.parquet").dropna()

    df_sessions["date_start"] = pd.to_datetime(df_sessions["date_start"])
    df_sessions["date_end"] = pd.to_datetime(df_sessions["date_end"])

    for _, row in tqdm(df_sessions.iterrows(), total=len(df_sessions)):
        point = (
            Point("sessions")
            .tag("session_key", str(row["session_key"]))
            .tag("meeting_key", str(row["meeting_key"]))
            .tag("location", str(row["location"]))
            .tag("circuit_short_name", str(row["circuit_short_name"]))
            .tag("session_type", str(row["session_type"]))  
            .tag("session_name", str(row["session_name"]))
            .field("year", row["year"])
            .time(row["date_start"], WritePrecision.NS)
        )
        write_api.write(bucket=bucket, org=org, record=point)
        
def insert_drivers():
    df_drivers = pd.read_parquet("./data/drivers/drivers.parquet").dropna()
    df_drivers = df_drivers.drop(columns=["meeting_key"])

    for _, row in tqdm(df_drivers.iterrows(), total=len(df_drivers)):
        point = (
            Point("drivers")
            .tag("driver_number", str(row["driver_number"]))
            .tag("session_key", str(row["session_key"]))
            .tag("broadcast_name", str(row["broadcast_name"]))
            .tag("full_name", str(row["full_name"]))
            .tag("name_acronym", str(row["name_acronym"]))
            .tag("team_name", str(row["team_name"]))
            .tag("team_colour", str(row["team_colour"]))
            .tag("first_name", str(row["first_name"]))
            .tag("last_name", str(row["last_name"]))
            .tag("headshot_url", str(row["headshot_url"]))
            .tag("country_code", str(row["country_code"]))
            .time(pd.Timestamp.utcnow(), WritePrecision.NS)
        )
        write_api.write(bucket=bucket, org=org, record=point)

def insert_pits():
    df_pits = pd.read_parquet("./data/pits/pits.parquet").dropna()
    df_pits = df_pits.drop(columns=["meeting_key"])

    df_pits["date"] = pd.to_datetime(df_pits["date"], format='ISO8601', utc=True)

    for _, row in tqdm(df_pits.iterrows(), total=len(df_pits)):
        point = (
            Point("pits")
            .tag("driver_number", str(row["driver_number"]))
            .tag("session_key", str(row["session_key"]))
            .tag("lap_number", str(row["lap_number"]))
            .field("pit_duration", float(row["pit_duration"]))
            .time(row["date"], WritePrecision.NS)
        )
        write_api.write(bucket=bucket, org=org, record=point)

def insert_positions():
    df_positions = pd.read_parquet("./data/positions/positions.parquet").dropna()
    df_positions = df_positions.drop(columns=["meeting_key"])
    df_positions = df_positions.drop_duplicates(subset=["session_key", "driver_number", "date"])

    df_positions["date"] = pd.to_datetime(df_positions["date"], format='ISO8601', utc=True)

    for _, row in tqdm(df_positions.iterrows(), total=len(df_positions)):
        point = (
            Point("positions")
            .tag("driver_number", str(row["driver_number"]))
            .tag("session_key", str(row["session_key"]))
            .field("position", int(row["position"]))
            .time(row["date"], WritePrecision.NS)
        )
        write_api.write(bucket=bucket, org=org, record=point)
    
def insert_telemetrys():
    path_telemetrys = "./data/telemetrys"
    if not os.path.isdir(path_telemetrys):
        raise FileNotFoundError(f"Directory does not exist: {path_telemetrys}")

    files = [f for f in os.listdir(path_telemetrys) if f.endswith(".parquet")]

    Parallel(n_jobs=-1)(
        delayed(process_telemetry)(os.path.join(path_telemetrys, file))
        for file in files
    )

def process_telemetry(file_path):
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        df_telemetry = pd.read_parquet(file_path).dropna()
        print(f"Processing file: {file_path} {df_telemetry.shape}")

        if "meeting_key" in df_telemetry.columns:
            df_telemetry = df_telemetry.drop(columns=["meeting_key"])
        
        df_telemetry = df_telemetry.drop_duplicates(subset=["session_key", "driver_number", "date"])
        
        df_telemetry["date"] = pd.to_datetime(df_telemetry["date"], format='ISO8601', utc=True)

        for _, row in df_telemetry.iterrows():
            point = (
                Point("telemetry")
                .tag("driver_number", str(row["driver_number"]))
                .tag("session_key", str(row["session_key"]))
                .field("rpm", int(row["rpm"]))
                .field("speed", int(row["speed"]))
                .field("n_gear", int(row["n_gear"]))
                .field("throttle", int(row["throttle"]))
                .field("brake", int(row["brake"]))
                .field("drs", int(row["drs"]))
                .time(row["date"], WritePrecision.NS)
            )
            write_api.write(bucket=bucket, org=org, record=point)
    except:
        print(f"Error processing file {file_path}")

if __name__ == "__main__":
    insert_tyre_strints()
    insert_weather_conditions()
    insert_meets()
    insert_sessions()
    insert_drivers()
    insert_pits()
    insert_positions()
    insert_telemetrys()

    client.close()
    print("Data insertion completed.")