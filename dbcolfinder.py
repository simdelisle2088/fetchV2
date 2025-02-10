from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import traceback
import warnings
import logging
import pyodbc
import os

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

# ====== Constants ====================
TABLE_NAME = "m.POS_TRX_HEADER POS_TRX_HEADER"

# ====== Main =========================
logging.info("=======================================")
logging.info("----------- ODBC is started -----------")
logging.info("=======================================\n")

# ====== Database Connection =============
def connect_to_database():
    try:
        return pyodbc.connect(DATABASE_URL, autocommit=True)
    except pyodbc.Error as e:
        logging.error(f"Error connecting to database: {e}")
        return None

# ====== Fetch All Columns ========
def fetch_all_columns(connection, table_name):
    try:
        query = f"""
            SELECT 
                m.POS_TRX_HEADER.Store,
                m.POS_TRX_HEADER.Tran_ToStore,
                m.POS_TRX_HEADER.TrxNo,
                m.POS_TRX_HEADER.TrxType,
                m.POS_TRX_HEADER.Customer,
                m.POS_TRX_HEADER.OrderNumber,
                m.POS_TRX_HEADER.DateCreation,
                m.POS_TRX_HEADER.Clerk,
                m.POS_TRX_HEADER.TotalAtCost,
                m.POS_TRX_HEADER.LastUpdTime,
                POS_TRX_SKU.Description,
                POS_TRX_SKU.QtySellingUnits,
                POS_TRX_SKU.SKU,
                m.IN_UPC.UPC
            FROM 
                m.POS_TRX_HEADER
            INNER JOIN 
                POS_TRX_SKU ON m.POS_TRX_HEADER.TrxNo = POS_TRX_SKU.TrxNo
            INNER JOIN 
                m.IN_UPC ON POS_TRX_SKU.SKU = m.IN_UPC.SKU
            WHERE 
                m.POS_TRX_HEADER.TrxNo = '797977'

            """
        data_frame = pd.read_sql_query(query, connection)
        
        # Use Pandas to limit the DataFrame to the first 5 rows
        data_frame = data_frame.head(100)
        logging.info("Successfully fetched all rows and limited to 5 rows in DataFrame.")
        
        return data_frame
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        logging.error(traceback.print_exc())
        return None


# Fetch data once and print
try:
    # Connect to the database
    connection = connect_to_database()
    if connection is None:
        raise Exception("Failed to connect to the database.")
    
    # Fetch all columns
    results = fetch_all_columns(connection, TABLE_NAME)
    
    if results is not None and not results.empty:
        # Convert DataFrame to string format for display
        data_str = results.to_string(index=False)
        
        # Print the data to the console
        print(data_str)
        
        logging.info("Data printed to the console successfully.\n")
    else:
        logging.info("No data fetched.\n")

except Exception as e:
    logging.error(f"\nGeneral Error: {e}\n")
    logging.error(traceback.print_exc())
finally:
    if connection:
        connection.close()
