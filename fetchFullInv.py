import base64
from datetime import datetime
import httpx
import pandas as pd
import traceback
import warnings
import logging
import pyodbc
import time

from fetch import ENCRYPTION_SUITE, HEADER, URL, connect_to_database
from queries import QUERY_GET_FULL_INVENTORY

# ====== Logging ===================
# warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")
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

    results = pd.read_sql_query(QUERY_GET_FULL_INVENTORY, connection)
    FetchEnd = time.perf_counter() # End Time

    logging.info(f"Data fetched successfully! {len(results)} rows collected.")
    logging.info(f"Finished in {(FetchEnd - FetchStart)/60:0.0f} minutes {(FetchEnd - FetchStart)%60:0.0f} seconds")

    results = results.drop(columns=['id'], errors='ignore')
    results = results.drop_duplicates()
    logging.info(f"Removed duplicates. {len(results)} rows left.")

    if not results.empty:

        chunk_size = 500000
        chunks = [results[i:i+chunk_size] for i in range(0, results.shape[0], chunk_size)]
        for chunk in chunks:
            logging.info("Compressing..")
            #Converting data from DataFrame to CSV to allow the compression
            csv_results = chunk.to_csv(index=False).encode('utf-8')
    
            logging.info("Encrypting..")
            encrypted_results = ENCRYPTION_SUITE.encrypt(csv_results)
    
            # Create a synchronous HTTP client and send data
            with httpx.Client(timeout=60) as client:
                data = {'data': base64.b64encode(encrypted_results).decode('utf-8')}
                response = client.post(URL+"/inv", json=data, headers=HEADER)
    else:
        logging.info("No data fetched.\n")

except pyodbc.Error as e:
    logging.error(f"An unexpected database error occurred: {e}\n")
    logging.error(traceback.print_exc())
    time.sleep(10)

except Exception as e:
    logging.error(f"\nGeneral Error: {e}\n")
    logging.error(traceback.print_exc())
    time.sleep(10)

finally:
    if connection and not connection.closed:
        connection.close()