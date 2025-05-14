import sys
import pandas as pd
import os
from sqlalchemy import create_engine
from joblib import Parallel, delayed

DATABASE_URL = "postgresql+psycopg2://postgresadmin:admin123@localhost:5000/postgresdb"

def insert_data_to_db(df, table_name, schema_name, engine, show=True):
    """
    Inserts a DataFrame into a PostgreSQL table.

    Args:
        df (pd.DataFrame): The data to be inserted.
        table_name (str): Name of the target table.
        schema_name (str): Name of the database schema.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine to connect to the database.
    """
    df.to_sql(table_name, engine, schema=schema_name, if_exists="append", index=False)
    if show:
        print(f"{table_name} data inserted successfully.")

def generate_meets(schema_name, engine):
    """
    Loads the meetings dataset, appends a dummy row, and inserts it into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_meets = pd.read_parquet("./data/meetings/meetings.parquet")
    insert_data_to_db(df_meets, "meetings", schema_name, engine)

def generate_sessions(schema_name, engine):
    """
    Loads sessions data from parquet and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_sessions = pd.read_parquet("./data/sessions/sessions.parquet")
    insert_data_to_db(df_sessions, "sessions", schema_name, engine)

def generate_drivers(schema_name, engine):
    """
    Loads drivers data and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_drivers = pd.read_parquet("./data/drivers/drivers.parquet")
    df_drivers = df_drivers.drop(columns=["meeting_key"])
    insert_data_to_db(df_drivers, "drivers", schema_name, engine)

def generate_laps(schema_name, engine):
    """
    Loads laps data, processes segments, and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_laps = pd.read_parquet("./data/laps/laps.parquet")
    df_laps = df_laps.drop(columns=["meeting_key"])
    df_laps = df_laps[~df_laps["driver_number"].isin([31])]
    df_laps = df_laps.drop(columns=["segments_sector_1", "segments_sector_2", "segments_sector_3"])

    insert_data_to_db(df_laps, "laps", schema_name, engine)

def generate_pits(schema_name, engine):
    """
    Loads pit stop data and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_pits = pd.read_parquet("./data/pits/pits.parquet")
    df_pits = df_pits.drop(columns=["meeting_key"])
    insert_data_to_db(df_pits, "pits", schema_name, engine)

def generate_positions(schema_name, engine):
    """
    Loads position data, removes duplicates, and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_positions = pd.read_parquet("./data/positions/positions.parquet")
    df_positions = df_positions.drop(columns=["meeting_key"])
    df_positions = df_positions.drop_duplicates(subset=["session_key", "driver_number", "date"])
    insert_data_to_db(df_positions, "positions", schema_name, engine)

def generate_weather(schema_name, engine):
    """
    Loads weather condition data, removes duplicates, and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_weather = pd.read_parquet("./data/weather_conditions/weather_conditions.parquet")
    df_session = pd.read_parquet("./data/sessions/sessions.parquet")
    df_weather = df_weather.drop(columns=["meeting_key"])
    df_weather = df_weather.drop_duplicates(subset=["session_key", "date"])
    df_weather = df_weather[df_weather["session_key"].isin(df_session["session_key"].to_list())]
    df_weather["rainfall"] = df_weather["rainfall"].astype(bool)
    
    insert_data_to_db(df_weather, "weather_conditions", schema_name, engine)

def generate_tyre_strits(schema_name, engine): 
    """
    Processes tyre_strits.

    Args:
        file_path (str): Path to the telemetry parquet file.
        schema_name (str): Database schema name.
    """
    df_tyre_strints = pd.read_parquet("./data/stints/stints.parquet")
    df_tyre_strints = df_tyre_strints.drop(columns=["meeting_key"])
    df_tyre_strints = df_tyre_strints.drop_duplicates(subset=["session_key", "stint_number", "driver_number"])

    insert_data_to_db(df_tyre_strints, "tyre_stints", schema_name, engine)

def process_telemetry(file_path, schema_name):
    """
    Processes a single telemetry file and inserts it into the database.

    Args:
        file_path (str): Path to the telemetry parquet file.
        schema_name (str): Database schema name.
    """
    engine = create_engine(DATABASE_URL)

    try:
        print(f"Processing file: {file_path}")
        df_telemetry = pd.read_parquet(file_path)

        if "meeting_key" in df_telemetry.columns:
            df_telemetry = df_telemetry.drop(columns=["meeting_key"])

        df_telemetry = df_telemetry.drop_duplicates(subset=["session_key", "driver_number", "date"])

        insert_data_to_db(df_telemetry, "telemetrys", schema_name, engine, show=False)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def generate_telemetrys(schema_name):
    """
    Processes all telemetry files in parallel and inserts them into the database.

    Args:
        schema_name (str): Database schema name.
    """
    path_telemetrys = "./data/telemetrys"
    if not os.path.isdir(path_telemetrys):
        raise FileNotFoundError(f"Directory does not exist: {path_telemetrys}")

    files = [f for f in os.listdir(path_telemetrys) if f.endswith(".parquet")]

    Parallel(n_jobs=-1)(
        delayed(process_telemetry)(os.path.join(path_telemetrys, file), schema_name)
        for file in files
    )

if __name__ == "__main__":
    schema_name = sys.argv[1]
    engine = create_engine(DATABASE_URL)

    generate_meets(schema_name, engine)
    generate_sessions(schema_name, engine)
    generate_drivers(schema_name, engine)
    generate_telemetrys(schema_name)
    generate_laps(schema_name, engine)
    generate_pits(schema_name, engine)
    generate_positions(schema_name, engine)
    generate_tyre_strits(schema_name, engine)
    generate_weather(schema_name, engine)
    print("All data has been successfully inserted into the database.")