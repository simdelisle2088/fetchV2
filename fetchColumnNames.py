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
from queries import QUERY_GET_FULL_INVENTORY, QUERY_TEST

# ====== Logging ===================
# warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")
FetchStart = time.perf_counter() # Start Time
logging.info("Fetching Table Colums Name...")
# ====== Environment Variables =======
try:
    # Get the time at the start of the loop
    start_time = time.time()
    current_time = datetime.now()
    # Connect to the database
    connection = connect_to_database()
    if connection is None:
        raise Exception("Failed to connect to the database.")
    arr = ["POS_TRX_SKU"]
    for table_name in arr:
        try:
            print("=" * 69)
            print(table_name)
            print("=" * 69)
            results = pd.read_sql_query(f"SELECT * FROM {table_name} WHERE 1=0", connection)
            
            print(', '.join(results.columns.tolist()))

        except Exception as e:
            logging.error(e)

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