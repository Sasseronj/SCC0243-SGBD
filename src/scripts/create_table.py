import sys
from sqlalchemy import create_engine, text
from dotenv import dotenv_values

DATABASE_URL = dotenv_values(".env.local")['DATABASE_URL']

if __name__ == "__main__":
    schema_name = sys.argv[1]

    schema_sql = f"""
        DROP TABLE IF EXISTS {schema_name}.race_controls;
        DROP TABLE IF EXISTS {schema_name}.weather_conditions;
        DROP TABLE IF EXISTS {schema_name}.tyre_stints;
        DROP TABLE IF EXISTS {schema_name}.positions;
        DROP TABLE IF EXISTS {schema_name}.pits;
        DROP TABLE IF EXISTS {schema_name}.laps;
        DROP TABLE IF EXISTS {schema_name}.telemetrys;
        DROP TABLE IF EXISTS {schema_name}.sessions;
        DROP TABLE IF EXISTS {schema_name}.meetings;
        DROP TABLE IF EXISTS {schema_name}.drivers;
        
        CREATE SCHEMA IF NOT EXISTS {schema_name};
        
        CREATE TABLE IF NOT EXISTS {schema_name}.drivers (
            driver_number INT,
            session_key INT,
            broadcast_name VARCHAR,
            country_code VARCHAR,
            first_name VARCHAR,
            full_name VARCHAR,
            headshot_url VARCHAR,
            last_name VARCHAR,
            name_acronym VARCHAR,
            team_colour VARCHAR,
            team_name VARCHAR, 
            PRIMARY KEY (driver_number, session_key)
        );

        CREATE TABLE {schema_name}.meetings (
            meeting_key INT,
            meeting_name VARCHAR,
            meeting_official_name VARCHAR,
            circuit_key INT,
            circuit_short_name VARCHAR,
            country_key INT,
            country_code VARCHAR,
            country_name VARCHAR,
            date_start TIMESTAMP,
            gmt_offset VARCHAR,
            location VARCHAR,
            year INT,
            meeting_code VARCHAR,
            PRIMARY KEY (meeting_key)
        );

        CREATE TABLE {schema_name}.sessions (
            session_key INT,
            meeting_key INT,
            session_name VARCHAR,
            session_type VARCHAR,
            date_start TIMESTAMP,
            date_end TIMESTAMP,
            circuit_key INT,
            circuit_short_name VARCHAR,
            country_key INT,
            country_code VARCHAR,
            country_name VARCHAR,
            location VARCHAR,
            gmt_offset VARCHAR,
            year INT,
            PRIMARY KEY (session_key),
            FOREIGN KEY (meeting_key) REFERENCES {schema_name}.meetings (meeting_key)
        );

        CREATE TABLE {schema_name}.telemetrys (
            session_key INT,
            driver_number INT,
            date TIMESTAMP,
            brake INT,
            drs INT,
            n_gear INT,
            rpm INT,
            speed FLOAT,
            throttle FLOAT,
            PRIMARY KEY (session_key, driver_number, date),
            FOREIGN KEY (session_key) REFERENCES {schema_name}.sessions (session_key),
            FOREIGN KEY (driver_number, session_key) REFERENCES {schema_name}.drivers (driver_number, session_key)
        );

        CREATE TABLE {schema_name}.laps (
            session_key INT,
            driver_number INT,
            date_start TIMESTAMP,
            duration_sector_1 FLOAT,
            duration_sector_2 FLOAT,
            duration_sector_3 FLOAT,
            i1_speed FLOAT,
            i2_speed FLOAT,
            is_pit_out_lap BOOLEAN,
            lap_duration FLOAT,
            lap_number INT,
            st_speed FLOAT,
            PRIMARY KEY (session_key, driver_number, lap_number),
            FOREIGN KEY (session_key) REFERENCES {schema_name}.sessions (session_key),
            FOREIGN KEY (driver_number, session_key) REFERENCES {schema_name}.drivers (driver_number, session_key)
        );

        CREATE TABLE {schema_name}.pits (
            session_key INT,
            driver_number INT,
            date TIMESTAMP,
            lap_number INT,
            pit_duration FLOAT,
            PRIMARY KEY (session_key, driver_number, lap_number),
            FOREIGN KEY (session_key) REFERENCES {schema_name}.sessions (session_key),
            FOREIGN KEY (driver_number, session_key) REFERENCES {schema_name}.drivers (driver_number, session_key)
        );

        CREATE TABLE {schema_name}.positions (
            session_key INT,
            driver_number INT,
            date TIMESTAMP,
            position INT,
            PRIMARY KEY (session_key, driver_number, date),
            FOREIGN KEY (session_key) REFERENCES {schema_name}.sessions (session_key),
            FOREIGN KEY (driver_number, session_key) REFERENCES {schema_name}.drivers (driver_number, session_key)
        );

        CREATE TABLE {schema_name}.tyre_stints (
            session_key INT,
            driver_number INT,
            stint_number INT,
            lap_start INT,
            lap_end INT,
            compound VARCHAR,
            tyre_age_at_start INT,
            PRIMARY KEY (session_key, driver_number, stint_number),
            FOREIGN KEY (session_key) REFERENCES {schema_name}.sessions (session_key),
            FOREIGN KEY (driver_number, session_key) REFERENCES {schema_name}.drivers (driver_number, session_key)
        );

        CREATE TABLE {schema_name}.weather_conditions (
            session_key INT,
            date TIMESTAMP,
            air_temperature FLOAT,
            humidity FLOAT,
            pressure FLOAT,
            rainfall BOOLEAN,
            track_temperature FLOAT,
            wind_direction FLOAT,
            wind_speed FLOAT,
            PRIMARY KEY (session_key, date),
            FOREIGN KEY (session_key) REFERENCES {schema_name}.sessions (session_key)
        );

        CREATE TABLE {schema_name}.race_controls (
            session_key INT,
            driver_number INT,
            category VARCHAR,
            date TIMESTAMP,
            flag VARCHAR,
            lap_number INT,
            message TEXT,
            scope VARCHAR,
            sector INT,
            PRIMARY KEY (session_key, driver_number, date),
            FOREIGN KEY (session_key) REFERENCES {schema_name}.sessions (session_key),
            FOREIGN KEY (driver_number, session_key) REFERENCES {schema_name}.drivers (driver_number, session_key)
        );
    """
    
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        for statement in schema_sql.strip().split(";"):
            if statement.strip():
                conn.execute(text(statement))
        conn.commit()