from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from dotenv import load_dotenv
import pandas as pd
import subprocess
import traceback
import warnings
import logging
import pyodbc
import pickle
import httpx
import time
import os

from queries import QUERY_GET_FULL_ORDER, QUERY_GET_UPC

# ====== Logging ===================
warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")

# Set up the root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(handler)

# ====== Environment Variables =======
load_dotenv()
ODBC_USER = os.getenv("ODBC_USER")
ODBC_PASSWORD = os.getenv("ODBC_PASSWORD")
ODBC_DSN = os.getenv("ODBC_DSN")
DATABASE_URL = f"DSN={ODBC_DSN};UID={ODBC_USER};PWD={ODBC_PASSWORD}"

CUSTOMER_EXCLUDE = set(os.getenv("CUSTOMER_EXCLUDE", "").split(","))
CLERK_EXCLUDE = set(os.getenv("CLERK_EXCLUDE", "").split(","))

PARSER_KEY = os.getenv("PARSER_KEY")
ENCRYPTION_SUITE = Fernet(os.getenv("ENCRYPTION_KEY").encode())

URL = "https://parserv2.pasuper.xyz"
HEADER = {"PARSER_KEY": PARSER_KEY}

# ====== Database ====================
def connect_to_database():
    try:
        return pyodbc.connect(DATABASE_URL, autocommit=True)
    except pyodbc.Error as e:
        logging.error(f"Error connecting to database: {e}")
        return None

# ====== Main =========================
if __name__ == "__main__":
    # Log startup message only once
    logging.info("=======================================")
    logging.info("----------- ODBC is started -----------")
    logging.info("=======================================\n")

    # Initialize variables
    past_time = None
    processed_docnos = set()  # Track processed DocNo values

    QUERY = QUERY_GET_FULL_ORDER

    # Main loop
    while True:
        try:
            # Get the time at the start of the loop
            start_time = time.time()

            # Get current time
            current_time = datetime.now()

            # Connect to the database
            connection = connect_to_database()
            if connection is None:
                raise Exception("Failed to connect to the database.")

            # For the first run, restart, errors, or daily database maintenance. Fetch data from the past 10 minutes
            if past_time is None:
                past_time = current_time - timedelta(seconds=600)
            else:
                past_time = current_time - timedelta(seconds=45)

            # Concatenating into a single integer in the format HHMMSS
            past_time_integer = (
                past_time.hour * 10000 + past_time.minute * 100 + past_time.second
            )

            # Convert time to date
            past_date = past_time.date()
            results = pd.read_sql_query(
                QUERY, connection, params=[past_date, past_time_integer, past_date]
            )

            # logging.info("Data fetched successfully.")
            if not results.empty:

                # Convert the date and time columns to datetime
                results["DocDate"] = pd.to_datetime(results["DocDate"])

                # Filter out unwanted data
                results = results[
                    ~results["Customer"].isin(CUSTOMER_EXCLUDE)
                    & ~results["Clerk"].isin(CLERK_EXCLUDE)
                    & (results["LineCode"] != "XFR")
                    & (~results["Description"].str.startswith("Core"))
                    & (results["PriceExtended"] >= 0)
                ].copy()

                upc_list = []

                for sku in results["SKU"].values:
                    results_upc = pd.read_sql_query(
                        QUERY_GET_UPC.format(str(sku)), connection
                    )
                    # Trim whitespace from each UPC and convert the column to a list
                    upcs = results_upc["upc"].str.strip().tolist()
                    upc_list.append(upcs)

                # Add the UPC list as a new column in the results DataFrame
                results["upc"] = upc_list

                # Filter out records that have already been processed
                results = results[~results["DocNo"].isin(processed_docnos)]

                if not results.empty:
                    # Add processed DocNo to the set to avoid reprocessing
                    processed_docnos.update(results["DocNo"].tolist())

                    # Create a synchronous HTTP client and send data
                    with httpx.Client() as client:
                        # Serialize and encrypt the data
                        encrypted_data = ENCRYPTION_SUITE.encrypt(pickle.dumps(results))
                        files = {"file": ("data.pkl", encrypted_data)}
                        # Log the encrypted data size
                        logging.debug(f"Encrypted Data Size: {len(encrypted_data)} bytes")
                        # Send the POST request and log the response
                        response = client.post(URL, files=files, headers=HEADER)

                    # Check the response
                    if response.status_code == 200:
                        logging.info("Data sent successfully.\n")
                    else:
                        logging.info(
                            f"Failed to send data. Status code: {response.status_code}, Response: {response.text}\n"
                        )
                else:
                    logging.info("No new filtered unique data found.\n")
            else:
                logging.info("No data fetched.\n")

        except pyodbc.Error as e:
            # Check for the specific error code
            if "-10061" in str(e):
                logging.warning(
                    "Taking a 10-second break due to daily database maintenance...\n"
                )
            else:
                logging.error(f"An unexpected database error occurred: {e}\n")
                logging.error(traceback.print_exc())
            # Reset past_time to fetch data from the past 10 minutes and take a 10-second break
            past_time = None
            time.sleep(10.0)
        except Exception as e:
            logging.error(f"\nGeneral Error: {e}\n")
            logging.error(traceback.print_exc())

            # Reset past_time to fetch data from the past 10 minutes and take a 10-second break
            past_time = None
            time.sleep(10.0)
        finally:
            if connection:
                connection.close()
            time.sleep(1.25)
