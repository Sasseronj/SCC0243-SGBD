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
        SELECT 
            tlm.session_key, 
            tlm.driver_number,
            CASE 
                WHEN tlm.date > laps.date_start AND tlm.date < laps.date_start + INTERVAL '1 second' * laps.duration_sector_1 THEN 'SECTOR 1'
                WHEN tlm.date > (laps.date_start + INTERVAL '1 second' * laps.duration_sector_1) AND tlm.date < (laps.date_start + INTERVAL '1 second' * (laps.duration_sector_1 + laps.duration_sector_2)) THEN 'SECTOR 2'
                WHEN tlm.date > (laps.date_start + INTERVAL '1 second' * (laps.duration_sector_1 + laps.duration_sector_2)) AND (tlm.date <= laps.date_start + INTERVAL '1 second' * (laps.duration_sector_1 + laps.duration_sector_2 + laps.duration_sector_3)) THEN 'SECTOR 3'
            END AS sector,
            MIN(laps.lap_duration) AS lap_duration,
            AVG(tlm.speed) AS max_speed
        FROM {schema_name}.telemetrys tlm
        JOIN (
            SELECT DISTINCT ON (driver_number)
                session_key,
                driver_number,
                lap_duration,
                date_start,
                duration_sector_1,
                duration_sector_2,
                duration_sector_3
            FROM {schema_name}.laps
            WHERE session_key in (
                SELECT 
                    session_key
                FROM {schema_name}.sessions
                WHERE session_name = 'Race'
            )
            ORDER BY driver_number, lap_duration ASC
        ) laps
        ON tlm.session_key = laps.session_key AND tlm.driver_number = laps.driver_number
        AND tlm.date BETWEEN laps.date_start AND laps.date_start + (INTERVAL '1 second' * (laps.duration_sector_1 + laps.duration_sector_2 + laps.duration_sector_3))
        GROUP BY tlm.session_key, tlm.driver_number, sector
        ORDER BY tlm.session_key, tlm.driver_number, lap_duration, sector ASC;
    """
    
    return pd.read_sql(query, engine)


def second_query(engine) -> pd.DataFrame:
    """
    Função que retorna a segunda query definida pelo grupo:

    Ver qual piloto possui a melhor aceleração por setor da pista.

    @params:
        - engine
    
    @returns:
        - table_data: pd.DataFrame
    """

    query = """
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
            FROM telemetrys
            WHERE session_key = 9998
            WINDOW W AS (PARTITION BY driver_number ORDER BY date)
        ) AS T
        INNER JOIN laps AS L ON L.session_key = T.session_key AND L.driver_number = T.driver_number
        GROUP BY T.driver_number, T.session_key, Sector
        ORDER BY AceleracaoMediaPorSetor DESC, T.driver_number ASC;
    """

    return pd.read_sql(query, engine)

def third_query(engine) -> pd.DataFrame:
    """
    Função que retorna a terceira query definida pelo grupo:

    Qual tipo de pneu apresenta o melhor desemepnho em relação a temperatura da pista.

    @params:
        - engine
    
    @returns:
        - table_data: pd.DataFrame
    """

    query = """
        SELECT  
            TS.compound AS CompostoPneu,
            AVG(WC.track_temperature)::NUMERIC(8,2) AS TemperaturaMediaPista,
            MAX(TS.lap_end - TS.lap_start) AS MaxLapDurationTyre
        FROM tyre_stints AS TS
        INNER JOIN weather_conditions AS WC ON WC.session_key = TS.session_key
        GROUP BY TS.compound
        ORDER BY MaxLapDurationTyre DESC;
    """

    return pd.read_sql(query, engine)

def fourth_query(engine) -> pd.DataFrame:
    """
    Função que retorna a quarta query definida pelo grupo.

    Para cada corrida e para cada corredor, obter a maximização da porcentagem média
    de consumo do carro antes do corredor parar em um pit-stop para todas as paradas de pit-stop
    e também mostrar qual foi o número de voltas associado a essa porcentagem média máxima. 
    """

def fifth_query(engine) -> pd.DataFrame:
    """
    Função que retorna a quinta query definida pelo grupo.

    Para cada corrida, cada piloto, obter a porcentagem média do uso do DRS e como isso está 
    correlacionado com a temperatura do ar naquela corrida. Caso a temperatura varie consideravelmente, 
    pegar a temperatura média da corrida.
    """


def main():
    schema_name = input("Digite aqui o nome do schema: ")
    engine = create_engine(DATABASE_URL)
 
    # df_second_result = second_query(engine)
    df_third_result = third_query(engine)

    print(df_third_result)



if __name__ == "__main__":
    main()