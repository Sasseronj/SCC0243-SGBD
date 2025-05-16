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


def first_query(engine) -> pd.DataFrame:
    """
    Função que retorna a primeira query definida pelo grupo:

    Considerando a melhor volta de cada piloto, encontrar qual a velocidade
    máxima alcançada em cada setor;

    @params:
        - engine
    
    @returns:
        - table_data: pd.DataFrame
    """

    query = """

        SELECT *
        FROM (
            SELECT L.lap_number AS lap_number, D.driver_number as driver_number, MAX(L.lap_duration)
            FROM laps AS L
            INNER JOIN drivers AS D ON L.driver_number = D.driver_number
            GROUP BY lap_number, driver_number
        )

    """

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