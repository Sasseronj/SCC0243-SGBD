import os
import pandas as pd

from dotenv import dotenv_values
from sqlalchemy import create_engine

DATABASE_URL = dotenv_values(".env.local")['DATABASE_URL']

def get_data_from_table(schema_name: str, query: str, engine) -> pd.DataFrame:
    """
    Gets data by specifying a query

    @params:
        - schema_name: str
        - query: str
    
    @returns:
        - table_data: pd.DataFrame
    """

    table_data = pd.read_sql(query, engine)

    return table_data

def get_all_drivers(schema_name: str, engine) -> pd.DataFrame:
    """
    Função que retorna todos os pilotos do banco de dados.

    @params:
        - schema_name: str
        - engine
    
    @returns:
        - table_data: pd.DataFrame
    """

    query = f"""
        SELECT 
            DISTINCT driver_number, 
            full_name, 
            country_code, 
            team_name
        FROM {schema_name}.drivers
        WHERE full_name IS NOT NULL
              AND country_code IS NOT NULL
              AND team_name IS NOT NULL
        ORDER BY driver_number ASC;
    """

    return pd.read_sql(query, engine)

def first_query(schema_name, engine) -> pd.DataFrame:
    """
    Função que retorna a primeira query definida pelo grupo:

    Considerando a melhor volta de cada piloto, encontrar qual a velocidade
    máxima alcançada em cada setor;

    @params:
        - schema_name
        - engine
    
    @returns:
        - table_data: pd.DataFrame
    """

    query = f"""
        WITH race_laps AS (
            SELECT
                session_key,
                driver_number,
                lap_number,
                date_start,
                lap_duration,
                duration_sector_1,
                duration_sector_2,
                duration_sector_3,
                ROW_NUMBER() OVER (PARTITION BY session_key, driver_number ORDER BY lap_duration ASC) AS rank_lap
            FROM raw.laps
            WHERE session_key in (
                SELECT session_key 
                FROM raw.sessions 
                WHERE session_name = 'Race'
            )
            AND lap_number IS NOT NULL	
        ),
        best_laps AS (
            SELECT *
            FROM race_laps
            WHERE rank_lap = 1
        ),
        filtred_telemetrys AS (
            SELECT 
                BL.*, 
                TLM.date,
                TLM.speed
            FROM raw.telemetrys TLM
            JOIN best_laps BL ON TLM.session_key = BL.session_key AND TLM.driver_number = BL.driver_number
            WHERE TLM.date BETWEEN BL.date_start AND BL.date_start + (INTERVAL '1 second' * (BL.duration_sector_1 + BL.duration_sector_2 + BL.duration_sector_3))
        ),
        avg_speed_per_sector AS (
            SELECT 
                session_key, 
                driver_number,
                MIN(lap_duration) AS best_lap_duration,
                CASE 
                    WHEN date > date_start AND date < date_start + INTERVAL '1 second' * duration_sector_1 THEN 'SECTOR 1'
                    WHEN date > (date_start + INTERVAL '1 second' * duration_sector_1) AND date < (date_start + INTERVAL '1 second' * (duration_sector_1 + duration_sector_2)) THEN 'SECTOR 2'
                    WHEN date > (date_start + INTERVAL '1 second' * (duration_sector_1 + duration_sector_2)) AND (date <= date_start + INTERVAL '1 second' * (duration_sector_1 + duration_sector_2 + duration_sector_3)) THEN 'SECTOR 3'
                END AS sector,
                AVG(speed) AS max_speed
            FROM filtred_telemetrys
            GROUP BY session_key, driver_number, sector
        )
        SELECT *
        FROM avg_speed_per_sector
    """
    
    return pd.read_sql(query, engine)


def second_query(schema_name, engine) -> pd.DataFrame:
    """
    Função que retorna a segunda query definida pelo grupo:

    Ver qual piloto possui a melhor aceleração por setor da pista.

    @params:
        - engine
    
    @returns:
        - table_data: pd.DataFrame
    """

    query = f"""
        SELECT DISTINCT
            T.driver_number,
            T.session_key,
            CASE
                WHEN T.date >= L.date_start AND T.date <= (L.date_start + INTERVAL '1 second' * L.duration_sector_1)::TIMESTAMP THEN 'SECTOR 1'
                WHEN T.date >= (L.date_start + INTERVAL '1 second' + INTERVAL '1 second' * L.duration_sector_1)::TIMESTAMP AND T.date <= (L.date_start + INTERVAL '1 second' * L.duration_sector_2)::TIMESTAMP THEN 'SECTOR 2'
                WHEN T.date >= (L.date_start + INTERVAL '1 second' + INTERVAL '1 second' * L.duration_sector_2)::TIMESTAMP AND T.date <= (L.date_start + INTERVAL '1 second' * L.duration_sector_3)::TIMESTAMP THEN 'SECTOR 3'
            END AS Sector,
            AVG(T.AceleracaoInstantanea) AS AceleracaoMediaPorSetor
        FROM (
            SELECT DISTINCT
                driver_number,
                session_key,
                date,
                CASE
                    WHEN EXTRACT (EPOCH FROM date - LAG(date, 1) OVER W) > 0 THEN ((speed - LAG(speed, 1) OVER W) / 3.6) / EXTRACT (EPOCH FROM date - LAG(date, 1) OVER W)
                    WHEN EXTRACT (EPOCH FROM date - LAG(date, 1) OVER W) = 0 THEN 0
                END AS AceleracaoInstantanea
            FROM {schema_name}.telemetrys
            WHERE session_key = 9998
            WINDOW W AS (PARTITION BY driver_number ORDER BY date)
        ) AS T
        INNER JOIN laps AS L ON L.session_key = T.session_key AND L.driver_number = T.driver_number
        GROUP BY T.driver_number, T.session_key, Sector
        ORDER BY AceleracaoMediaPorSetor DESC, T.driver_number ASC;
    """

    return pd.read_sql(query, engine)

def third_query(schema_name, engine) -> pd.DataFrame:
    """
    Função que retorna a terceira query definida pelo grupo:

    Qual tipo de pneu apresenta o melhor desemepnho em relação a temperatura da pista.

    @params:
        - engine
    
    @returns:
        - table_data: pd.DataFrame
    """

    query = f"""
        SELECT  
            TS.compound AS CompostoPneu,
            AVG(WC.track_temperature)::NUMERIC(8,2) AS TemperaturaMediaPista,
            MAX(TS.lap_end - TS.lap_start) AS MaxLapDurationTyre
        FROM {schema_name}.tyre_stints AS TS
        INNER JOIN {schema_name}.weather_conditions AS WC ON WC.session_key = TS.session_key
        GROUP BY TS.compound
        ORDER BY MaxLapDurationTyre DESC;
    """

    return pd.read_sql(query, engine)

def fourth_query(schema_name, engine) -> pd.DataFrame:
    """
    Função que retorna a quarta query definida pelo grupo.

    Análise do impacto do DRS na velocidade do carro em cada setor da pista, para cada piloto e para cada pista.
    """

    query = f"""
        WITH base AS (
            SELECT
                T.session_key,
                T.driver_number,
                T.date,
                T.speed,
                CASE 
                    WHEN T.brake = 100 THEN 'FREANDO'
                    ELSE 'NORMAL'
                END AS UsoFreio,
                CASE
                    WHEN T.drs IN (8, 10, 12, 14) THEN 'ATIVO'
                    ELSE 'NÃO ATIVO'
                    END AS drs,
                CASE
                WHEN T.date >= L.date_start AND T.date <= (L.date_start + INTERVAL '1 second' * L.duration_sector_1) THEN 'SETOR 1'
                WHEN T.date > (L.date_start + INTERVAL '1 second' * L.duration_sector_1) AND T.date <= (L.date_start + INTERVAL '1 second' * L.duration_sector_2) THEN 'SETOR 2'
                WHEN T.date > (L.date_start + INTERVAL '1 second' * L.duration_sector_2) AND T.date <= (L.date_start + INTERVAL '1 second' * L.duration_sector_3) THEN 'SETOR 3'
                END AS setor
            FROM {schema_name}.telemetrys AS T 
            JOIN {schema_name}.laps AS L ON L.session_key = T.session_key AND L.driver_number = T.driver_number
            WHERE 
                ((T.date >= L.date_start AND T.date <= (L.date_start + INTERVAL '1 second' * L.duration_sector_1)) OR
                (T.date > (L.date_start + INTERVAL '1 second' * L.duration_sector_1) AND T.date <= (L.date_start + INTERVAL '1 second' * L.duration_sector_2)) OR
                (T.date > (L.date_start + INTERVAL '1 second' * L.duration_sector_2) AND T.date <= (L.date_start + INTERVAL '1 second' * L.duration_sector_3))) AND T.session_key = 9998 AND T.driver_number = 30
            WINDOW W AS (ORDER BY date)
            ), com_setor AS (
                SELECT
                    *,
                    CASE
                        WHEN LAG(session_key) OVER W = session_key AND
                        LAG(driver_number) OVER W = driver_number AND
                        LAG(drs) OVER W = drs AND
                        LAG(setor) OVER W = setor AND
                        LAG(usofreio) OVER W = usofreio
                        THEN 0 
                        ELSE 1
                    END AS mudou
                FROM base
            WINDOW W AS (ORDER BY date)
            ),com_grupo AS (
                SELECT 
                    *,
                    SUM(mudou) OVER (PARTITION BY session_key, driver_number ORDER BY date) AS group_id
                FROM com_setor
            )
            SELECT DISTINCT
                S.circuit_short_name AS NomePista, 
                D.full_name AS NomePiloto,
                MIN(T1.date) AS TempoInicio,
                MAX(T1.date) AS TempoFim,
                T1.drs AS DRS,
                T1.setor AS Setor,
                T1.usofreio,
                T1.VelocidadeInicio,
                T2.VelocidadeFim
            FROM (
                SELECT DISTINCT
                    session_key,
                    driver_number,
                    date,
                    drs,
                    setor,
                    usofreio,
                    group_id,
                    FIRST_VALUE(speed) OVER W AS VelocidadeInicio
                FROM com_grupo
                WINDOW W AS (PARTITION BY group_id ORDER BY date)
            ) AS T1
            JOIN (
                SELECT DISTINCT
                    session_key,
                    driver_number,
                    date,
                    drs,
                    setor,
                    usofreio,
                    group_id,
                    LAST_VALUE(speed) OVER W AS VelocidadeFim
                FROM com_grupo
                WINDOW W AS (PARTITION BY group_id ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            ) AS T2 ON T1.session_key = T2.session_key AND T1.driver_number = T2.driver_number AND T1.usofreio = T2.usofreio AND T1.date = T2.date AND T1.drs = T2.drs AND T1.setor = T2.setor AND T1.group_id = T2.group_id
            JOIN {schema_name}.drivers AS D ON T1.session_key = D.session_key AND T1.driver_number = D.driver_number
            JOIN {schema_name}.sessions AS S ON S.session_key = T1.session_key
            GROUP BY S.circuit_short_name, D.full_name, T1.drs, T1.setor, T1.group_id, T1.VelocidadeInicio, T2.VelocidadeFim, T1.usofreio; 
    """

    return pd.read_sql(query, engine)


def fifth_query(schema_name, engine) -> pd.DataFrame:
    """
    Função que retorna a quinta query definida pelo grupo.

    Qual é a relação entre a temperatura média da pista em relação ao desempenho do carro em termos de velocidade média e uso médio do motor?
    """

    query = f"""
    SELECT
        S.circuit_short_name AS NomeCircuito,
        D.full_name AS NomeDoPiloto, 
        AVG(T.speed)::NUMERIC(8,2) AS VelocidadeMediaPiloto,
        AVG(T.throttle)::NUMERIC(8,2) AS ConsumoPotenciaMediaMotor,
        AVG(WC.track_temperature)::NUMERIC(8,2) AS TemperaturaMediaPista
    FROM {schema_name}.telemetrys AS T
    JOIN {schema_name}.drivers AS D ON T.session_key = D.session_key AND T.driver_number = D.driver_number
    JOIN {schema_name}.weather_conditions AS WC ON WC.session_key = T.session_key
    JOIN {schema_name}.sessions AS S ON S.session_key = T.session_key
    WHERE T.session_key = 9998
    GROUP BY S.circuit_short_name, D.full_name;
    """

    return pd.read_sql(query, engine)


def main():
    schema_name = input("Digite aqui o nome do schema: ")
    engine = create_engine(DATABASE_URL)
 
    # df_second_result = second_query(engine)
    df_fourth_result = fourth_query(engine)

    print(df_fourth_result)



if __name__ == "__main__":
    main()