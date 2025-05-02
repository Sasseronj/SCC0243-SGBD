import sys
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql+psycopg2://postgresadmin:admin123@localhost:5000/postgresdb"

if __name__ == "__main__":
    schema_name = sys.argv[1]

    schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};

        CREATE TABLE IF NOT EXISTS {schema_name}.drivers (
            nk_driver varchar PRIMARY KEY,
            driver_number int,
            session_key int,
            broadcast_name varchar,
            country_code varchar,
            first_name varchar,
            full_name varchar,
            headshot_url varchar,
            last_name varchar,
            name_acronym varchar,
            team_colour varchar,
            team_name varchar
        );

        CREATE TABLE IF NOT EXISTS {schema_name}.meetings (
            meeting_key int PRIMARY KEY,
            meeting_name varchar,
            meeting_code varchar,
            meeting_official_name varchar,
            circuit_key int,
            circuit_short_name varchar,
            country_key int,
            country_code varchar,
            country_name varchar,
            date_start timestamp,
            gmt_offset varchar,
            location varchar,
            year int
        );

        CREATE TABLE IF NOT EXISTS {schema_name}.sessions (
            session_key int PRIMARY KEY,
            meeting_key int REFERENCES {schema_name}.meetings(meeting_key),
            session_name varchar,
            session_type varchar,
            date_start timestamp,
            date_end timestamp,
            circuit_key int,
            circuit_short_name varchar,
            country_key int,
            country_code varchar,
            country_name varchar,
            location varchar,
            gmt_offset varchar,
            year int
        );

        CREATE TABLE IF NOT EXISTS {schema_name}.telemetrys (
            nk_driver varchar REFERENCES {schema_name}.drivers(nk_driver),
            session_key int REFERENCES {schema_name}.sessions(session_key),
            driver_number int,
            date timestamp,
            brake int,
            drs int,
            n_gear int,
            rpm int,
            speed float,
            throttle float,
            PRIMARY KEY (nk_driver, date)
        );

        CREATE TABLE IF NOT EXISTS {schema_name}.laps (
            nk_driver varchar REFERENCES {schema_name}.drivers(nk_driver),
            session_key int REFERENCES {schema_name}.sessions(session_key),
            driver_number int,
            date_start timestamp,
            duration_sector_1 float,
            duration_sector_2 float,
            duration_sector_3 float,
            i1_speed float,
            i2_speed float,
            is_pit_out_lap boolean,
            lap_duration float,
            lap_number int,
            segments_sector_1 float array,
            segments_sector_2 float array,
            segments_sector_3 float array,
            st_speed float,
            PRIMARY KEY (nk_driver, lap_number)
        );

        CREATE TABLE IF NOT EXISTS {schema_name}.pits (
            nk_driver varchar REFERENCES {schema_name}.drivers(nk_driver),
            session_key int REFERENCES {schema_name}.sessions(session_key),
            driver_number int,
            date timestamp,
            lap_number int,
            pit_duration float,
            PRIMARY KEY (nk_driver, lap_number)
        );

        CREATE TABLE IF NOT EXISTS {schema_name}.positions (
            nk_driver varchar REFERENCES {schema_name}.drivers(nk_driver),
            session_key int REFERENCES {schema_name}.sessions(session_key),
            driver_number int,
            date timestamp,
            position int,
            PRIMARY KEY (nk_driver, date)
        );

        CREATE TABLE IF NOT EXISTS {schema_name}.stints (
            nk_driver varchar REFERENCES {schema_name}.drivers(nk_driver),
            session_key int REFERENCES {schema_name}.sessions(session_key),
            driver_number int,
            stint_number int,
            lap_start int,
            lap_end int,
            compound varchar,
            tyre_age_at_start int,
            PRIMARY KEY (nk_driver, stint_number)
        );

        CREATE TABLE IF NOT EXISTS {schema_name}.weather_conditions (
            session_key int REFERENCES {schema_name}.sessions(session_key),
            date timestamp,
            air_temperature float,
            humidity float,
            pressure float,
            rainfall boolean,
            track_temperature float,
            wind_direction float,
            wind_speed float,
            PRIMARY KEY (session_key, date)
        );

        COMMENT ON COLUMN {schema_name}.drivers.nk_driver IS 'The natural is <driver_number>_<session_key>';
        COMMENT ON COLUMN {schema_name}.drivers.driver_number IS 'Unique number assigned to an F1 driver (cf. Wikipedia)';
        COMMENT ON COLUMN {schema_name}.drivers.session_key IS 'FK to session';
        COMMENT ON COLUMN {schema_name}.drivers.broadcast_name IS 'Driver''s name as shown on TV';
        COMMENT ON COLUMN {schema_name}.drivers.country_code IS 'Code identifying the driver’s country';
        COMMENT ON COLUMN {schema_name}.drivers.first_name IS 'Driver''s first name';
        COMMENT ON COLUMN {schema_name}.drivers.full_name IS 'Driver''s full name';
        COMMENT ON COLUMN {schema_name}.drivers.headshot_url IS 'URL of the driver''s headshot';
        COMMENT ON COLUMN {schema_name}.drivers.last_name IS 'Driver''s last name';
        COMMENT ON COLUMN {schema_name}.drivers.name_acronym IS 'Three-letter acronym for the driver';
        COMMENT ON COLUMN {schema_name}.drivers.team_colour IS 'Team''s hexadecimal color value (RRGGBB)';
        COMMENT ON COLUMN {schema_name}.drivers.team_name IS 'Name of the driver''s team';
        COMMENT ON COLUMN {schema_name}.meetings.meeting_key IS 'Unique identifier for the meeting';
        COMMENT ON COLUMN {schema_name}.meetings.meeting_name IS 'Name of the meeting';
        COMMENT ON COLUMN {schema_name}.meetings.meeting_official_name IS 'Official name of the meeting';
        COMMENT ON COLUMN {schema_name}.meetings.circuit_key IS 'Unique identifier for the circuit';
        COMMENT ON COLUMN {schema_name}.meetings.circuit_short_name IS 'Short or common name of the circuit';
        COMMENT ON COLUMN {schema_name}.meetings.country_key IS 'Unique identifier for the country';
        COMMENT ON COLUMN {schema_name}.meetings.country_code IS 'Code uniquely identifying the country';
        COMMENT ON COLUMN {schema_name}.meetings.country_name IS 'Full name of the country';
        COMMENT ON COLUMN {schema_name}.meetings.date_start IS 'UTC start date and time (ISO 8601)';
        COMMENT ON COLUMN {schema_name}.meetings.gmt_offset IS 'Offset between local time and GMT (e.g., +02:00)';
        COMMENT ON COLUMN {schema_name}.meetings.location IS 'City or location of the event';
        COMMENT ON COLUMN {schema_name}.meetings.year IS 'Year the event takes place';
        COMMENT ON COLUMN {schema_name}.sessions.session_key IS 'Unique identifier for the session';
        COMMENT ON COLUMN {schema_name}.sessions.meeting_key IS 'FK to meeting';
        COMMENT ON COLUMN {schema_name}.sessions.session_name IS 'Name of the session (Practice 1, Qualifying, Race, ...)';
        COMMENT ON COLUMN {schema_name}.sessions.session_type IS 'Type of the session (Practice, Qualifying, Race, ...)';
        COMMENT ON COLUMN {schema_name}.sessions.date_start IS 'UTC start date and time (ISO 8601 format)';
        COMMENT ON COLUMN {schema_name}.sessions.date_end IS 'UTC end date and time (ISO 8601 format)';
        COMMENT ON COLUMN {schema_name}.sessions.circuit_key IS 'Unique identifier for the circuit';
        COMMENT ON COLUMN {schema_name}.sessions.circuit_short_name IS 'Short or common name of the circuit';
        COMMENT ON COLUMN {schema_name}.sessions.country_key IS 'Unique identifier for the country';
        COMMENT ON COLUMN {schema_name}.sessions.country_code IS 'Country code';
        COMMENT ON COLUMN {schema_name}.sessions.country_name IS 'Full name of the country';
        COMMENT ON COLUMN {schema_name}.sessions.location IS 'City or location where the event takes place';
        COMMENT ON COLUMN {schema_name}.sessions.gmt_offset IS 'Offset between local time and GMT (e.g., +02:00)';
        COMMENT ON COLUMN {schema_name}.sessions.year IS 'Year the event takes place';
        COMMENT ON COLUMN {schema_name}.telemetrys.nk_driver IS 'The natural is <driver_number>_<session_key>';
        COMMENT ON COLUMN {schema_name}.telemetrys.session_key IS 'The unique identifier for the session';
        COMMENT ON COLUMN {schema_name}.telemetrys.driver_number IS 'The unique number assigned to an F1 driver';
        COMMENT ON COLUMN {schema_name}.telemetrys.date IS 'The UTC date and time, in ISO 8601 format';
        COMMENT ON COLUMN {schema_name}.telemetrys.brake IS 'Whether the brake pedal is pressed (100) or not (0)';
        COMMENT ON COLUMN {schema_name}.telemetrys.drs IS 'The Drag Reduction System (DRS) status';
        COMMENT ON COLUMN {schema_name}.telemetrys.n_gear IS 'Current gear selection, 0 = neutral';
        COMMENT ON COLUMN {schema_name}.telemetrys.rpm IS 'Revolutions per minute of the engine';
        COMMENT ON COLUMN {schema_name}.telemetrys.speed IS 'Velocity of the car in km/h';
        COMMENT ON COLUMN {schema_name}.telemetrys.throttle IS 'Percentage of maximum engine power being used';
        COMMENT ON COLUMN {schema_name}.laps.nk_driver IS 'The natural is <driver_number>_<session_key>';
        COMMENT ON COLUMN {schema_name}.laps.session_key IS 'FK to session';
        COMMENT ON COLUMN {schema_name}.laps.driver_number IS 'Unique number assigned to an F1 driver';
        COMMENT ON COLUMN {schema_name}.laps.date_start IS 'UTC starting date and time of the lap';
        COMMENT ON COLUMN {schema_name}.laps.duration_sector_1 IS 'Time in seconds to complete the first sector';
        COMMENT ON COLUMN {schema_name}.laps.duration_sector_2 IS 'Time in seconds to complete the second sector';
        COMMENT ON COLUMN {schema_name}.laps.duration_sector_3 IS 'Time in seconds to complete the third sector';
        COMMENT ON COLUMN {schema_name}.laps.i1_speed IS 'Speed in km/h at the first intermediate point';
        COMMENT ON COLUMN {schema_name}.laps.i2_speed IS 'Speed in km/h at the second intermediate point';
        COMMENT ON COLUMN {schema_name}.laps.is_pit_out_lap IS 'True if lap is an out lap from the pit';
        COMMENT ON COLUMN {schema_name}.laps.lap_duration IS 'Total lap time in seconds';
        COMMENT ON COLUMN {schema_name}.laps.lap_number IS 'Sequential lap number starting from 1';
        COMMENT ON COLUMN {schema_name}.laps.segments_sector_1 IS 'List of mini-sector values for sector 1';
        COMMENT ON COLUMN {schema_name}.laps.segments_sector_2 IS 'List of mini-sector values for sector 2';
        COMMENT ON COLUMN {schema_name}.laps.segments_sector_3 IS 'List of mini-sector values for sector 3';
        COMMENT ON COLUMN {schema_name}.laps.st_speed IS 'Speed in km/h at the speed trap';
        COMMENT ON COLUMN {schema_name}.pits.nk_driver IS 'The natural is <driver_number>_<session_key>';
        COMMENT ON COLUMN {schema_name}.pits.session_key IS 'FK to session';
        COMMENT ON COLUMN {schema_name}.pits.driver_number IS 'Unique number assigned to an F1 driver';
        COMMENT ON COLUMN {schema_name}.pits.date IS 'UTC date and time, in ISO 8601 format';
        COMMENT ON COLUMN {schema_name}.pits.lap_number IS 'Sequential lap number within the session (starts at 1)';
        COMMENT ON COLUMN {schema_name}.pits.pit_duration IS 'Time spent in the pit lane in seconds';
        COMMENT ON COLUMN {schema_name}.positions.nk_driver IS 'The natural is <driver_number>_<session_key>';
        COMMENT ON COLUMN {schema_name}.positions.session_key IS 'FK to session';
        COMMENT ON COLUMN {schema_name}.positions.driver_number IS 'Unique number assigned to an F1 driver';
        COMMENT ON COLUMN {schema_name}.positions.date IS 'UTC date and time, in ISO 8601 format';
        COMMENT ON COLUMN {schema_name}.positions.position IS 'Position of the driver (starts at 1)';
        COMMENT ON COLUMN {schema_name}.stints.nk_driver IS 'The natural is <driver_number>_<session_key>';
        COMMENT ON COLUMN {schema_name}.stints.session_key IS 'FK to session';
        COMMENT ON COLUMN {schema_name}.stints.driver_number IS 'Unique number assigned to an F1 driver';
        COMMENT ON COLUMN {schema_name}.stints.stint_number IS 'Sequential number of the stint within the session (starts at 1)';
        COMMENT ON COLUMN {schema_name}.stints.lap_start IS 'Number of the initial lap in this stint (starts at 1)';
        COMMENT ON COLUMN {schema_name}.stints.lap_end IS 'Number of the last completed lap in this stint';
        COMMENT ON COLUMN {schema_name}.stints.compound IS 'Specific compound of tyre used during the stint (SOFT, MEDIUM, HARD, ...)';
        COMMENT ON COLUMN {schema_name}.stints.tyre_age_at_start IS 'Age of the tyres at the start of the stint, in laps completed';
        COMMENT ON COLUMN {schema_name}.weather_conditions.session_key IS 'FK to session';
        COMMENT ON COLUMN {schema_name}.weather_conditions.date IS 'UTC date and time, in ISO 8601 format';
        COMMENT ON COLUMN {schema_name}.weather_conditions.air_temperature IS 'Air temperature in °C';
        COMMENT ON COLUMN {schema_name}.weather_conditions.humidity IS 'Relative humidity in %';
        COMMENT ON COLUMN {schema_name}.weather_conditions.pressure IS 'Air pressure in mbar';
        COMMENT ON COLUMN {schema_name}.weather_conditions.rainfall IS 'Whether there is rainfall';
        COMMENT ON COLUMN {schema_name}.weather_conditions.track_temperature IS 'Track temperature in °C';
        COMMENT ON COLUMN {schema_name}.weather_conditions.wind_direction IS 'Wind direction in °, from 0° to 359°';
        COMMENT ON COLUMN {schema_name}.weather_conditions.wind_speed IS 'Wind speed in m/s';
    """
    
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        for statement in schema_sql.strip().split(";"):
            if statement.strip():
                conn.execute(text(statement))
        conn.commit()