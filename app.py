from src.scripts.queries import (
    get_all_drivers, 
    first_query,
    second_query,
    third_query,
    fourth_query,
    fifth_query
)

from dotenv import dotenv_values
from sqlalchemy import create_engine
from tabulate import tabulate

import os
import time

DATABASE_URL = dotenv_values(".env.local")['DATABASE_URL']


def print_header() -> None:
    """
    Prints the application header and available query options.
    """
    header = "".join([
        "--------------------------------------------------------------------------------------\n",
        "------------------------------------- F1 Analisys ------------------------------------\n",
        "--------------------------------------------------------------------------------------\n",
        "1 - (Relatory) First query\n",
        "2 - (Relatory) Second query\n",
        "3 - (Relatory) Third query\n",
        "4 - (Relatory) Fourth query\n",
        "5 - (Relatory) Fifth query\n",
        "6 - List all drivers\n",
        "0 - Exit\n",
        "--------------------------------------------------------------------------------------\n"
    ]) 
    
    print(header)


def execute_command(engine) -> None:
    """
    Prompts the user for a command, executes the corresponding query, 
    and returns the result as a DataFrame along with the execution duration.
    
    Parameters:
        engine: SQLAlchemy engine used to connect to the database.

    Returns:
        tuple: (DataFrame, duration in seconds)
    """
    commands = {
        "1": first_query,
        "2": second_query,
        "3": third_query,
        "4": fourth_query,
        "5": fifth_query,
        "6": get_all_drivers,
        "0": "exit"
    }
    
    command = input("Enter your command: ")
    while command not in commands:
        print("Invalid command. Please try again.")
        command = input("Enter your command: ")
    
    print()
    
    if command == "0":
        exit()
    
    start = time.time()    
    dataframe = commands[command](schema_name="raw", engine=engine)
    end = time.time()
    
    return (dataframe, end - start)
    
def print_dataframe(dataframe, duration) -> None:
    """
    Prints basic information about the DataFrame and the execution time.

    Parameters:
        dataframe: The pandas DataFrame returned from the query.
        duration: Time (in seconds) it took to execute the query.
    """
    print(f"Sample of the data: {dataframe.shape}")
    print(f"Query executed in {duration:.2f} seconds\n")
    print(tabulate(dataframe.head(20), headers='keys', tablefmt='grid', showindex=False))


if __name__ == "__main__":
    # Create a database engine
    engine = create_engine(DATABASE_URL)
    
    # Application main loop
    while True:
        os.system('clear')  # Clear terminal screen (Linux/macOS)

        print_header()
        dataframe, duration = execute_command(engine)
        print_dataframe(dataframe, duration)
        
        input("\nPress Enter to continue...")
