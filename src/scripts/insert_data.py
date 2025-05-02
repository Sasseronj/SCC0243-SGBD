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

def create_nk_driver(row):
    """
    Generates a unique key for a driver using driver number and session key.

    Args:
        row (pd.Series): A row from a DataFrame.

    Returns:
        str: A unique driver identifier (nk_driver).
    """
    return f"{row['driver_number']}_{row['session_key']}"

def generate_meets(schema_name, engine):
    """
    Loads the meetings dataset, appends a dummy row, and inserts it into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_meets = pd.read_parquet('./data/meetings/meetings.parquet')
    new_row = {col: None for col in df_meets.columns}
    new_row['meeting_key'] = 1221
    df_meets.loc[len(df_meets)] = new_row
    insert_data_to_db(df_meets, "meetings", schema_name, engine)

def generate_sessions(schema_name, engine):
    """
    Loads sessions data from parquet and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_sessions = pd.read_parquet('./data/sessions/sessions.parquet')
    insert_data_to_db(df_sessions, "sessions", schema_name, engine)

def generate_drivers(schema_name, engine):
    """
    Loads drivers data, creates nk_driver, and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_drivers = pd.read_parquet('./data/drivers/drivers.parquet')
    df_drivers = df_drivers.drop(columns=['meeting_key'])
    df_drivers["nk_driver"] = df_drivers.apply(create_nk_driver, axis=1)
    insert_data_to_db(df_drivers, "drivers", schema_name, engine)

def generate_laps(schema_name, engine):
    """
    Loads laps data, creates nk_driver, processes segments, and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_laps = pd.read_parquet('./data/laps/laps.parquet')
    df_laps = df_laps.drop(columns=['meeting_key'])
    df_laps["nk_driver"] = df_laps.apply(create_nk_driver, axis=1)

    for col in ["segments_sector_1", "segments_sector_2", "segments_sector_3"]:
        df_laps[col] = df_laps[col].apply(list)

    insert_data_to_db(df_laps, "laps", schema_name, engine)

def generate_pits(schema_name, engine):
    """
    Loads pit stop data, creates nk_driver, and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_pits = pd.read_parquet('./data/pits/pits.parquet')
    df_pits = df_pits.drop(columns=['meeting_key'])
    df_pits["nk_driver"] = df_pits.apply(create_nk_driver, axis=1)
    insert_data_to_db(df_pits, "pits", schema_name, engine)

def generate_positions(schema_name, engine):
    """
    Loads position data, creates nk_driver, removes duplicates, and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_positions = pd.read_parquet('./data/positions/positions.parquet')
    df_positions = df_positions.drop(columns=['meeting_key'])
    df_positions["nk_driver"] = df_positions.apply(create_nk_driver, axis=1)
    df_positions = df_positions.drop_duplicates(subset=['nk_driver', 'date'])
    insert_data_to_db(df_positions, "positions", schema_name, engine)

def generate_weather(schema_name, engine):
    """
    Loads weather condition data, removes duplicates, and inserts into the database.

    Args:
        schema_name (str): Database schema name.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine.
    """
    df_weather = pd.read_parquet('./data/weather_conditions/weather_conditions.parquet')
    df_weather = df_weather.drop(columns=['meeting_key'])
    df_weather = df_weather.drop_duplicates(subset=['session_key', 'date'])
    insert_data_to_db(df_weather, "weather", schema_name, engine)

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

        if 'meeting_key' in df_telemetry.columns:
            df_telemetry = df_telemetry.drop(columns=['meeting_key'])

        df_telemetry["nk_driver"] = df_telemetry.apply(create_nk_driver, axis=1)
        df_telemetry = df_telemetry.drop_duplicates(subset=['nk_driver', 'date'])

        insert_data_to_db(df_telemetry, "telemetrys", schema_name, engine, show=False)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def generate_telemetrys(schema_name):
    """
    Processes all telemetry files in parallel and inserts them into the database.

    Args:
        schema_name (str): Database schema name.
    """
    path_telemetrys = './data/telemetrys'
    if not os.path.isdir(path_telemetrys):
        raise FileNotFoundError(f"Directory does not exist: {path_telemetrys}")

    files = [f for f in os.listdir(path_telemetrys) if f.endswith('.parquet')]

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
    generate_weather(schema_name, engine)
    print("All data has been successfully inserted into the database.")