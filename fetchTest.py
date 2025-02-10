from datetime import datetime
import pandas as pd
import traceback
import logging
import pyodbc
import time

from fetch import connect_to_database
from queries import QUERY_TRANSFERS

# ====== Logging ===================
FetchStart = time.perf_counter() # Start Time
logging.info("Fetching inventory Data...")
# ====== Environment Variables =======
try:
    # Get the time at the start of the loop
    start_time = time.time()
    current_time = datetime.now()
    # Connect to the database
    connection = connect_to_database()
    if connection is None:
        raise Exception("Failed to connect to the database.")

    # Read the query into a pandas DataFrame
    results = pd.read_sql_query(QUERY_TRANSFERS, connection)
    FetchEnd = time.perf_counter() # End Time

    # Drop unnecessary columns and remove duplicates
    results = results.drop(columns=['id'], errors='ignore')
    results = results.drop_duplicates()
    logging.info(f"Removed duplicates. {len(results)} rows left.")

    # Define the CSV file path
    csv_file_path = 'clients_data.csv'  # You can modify the path to suit your project structure

    # Save the DataFrame to a CSV file if data is not empty
    if not results.empty:
        results.to_csv(csv_file_path, index=False)
        logging.info(f"CSV file saved at: {csv_file_path}")
        print(results)
    else:
        logging.info("No data fetched.\n")

except pyodbc.Error as e:
    logging.error(f"An unexpected database error occurred: {e}\n")
    logging.error(traceback.format_exc())
    time.sleep(10)

except Exception as e:
    logging.error(f"\nGeneral Error: {e}\n")
    logging.error(traceback.format_exc())
    time.sleep(10)

finally:
    if connection and not connection.closed:
        connection.close()
