import azure.functions as func
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import logging



# 1. This line creates the "app" that the Azure tools look for.
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)



# 2. This is your new "decorator". It's an HTTP Trigger.
# It will run when you visit the URL: .../api/load-data-to-snowflake
@app.route(route="load-data-to-snowflake")
def HttpTriggerToSnowflake(req: func.HttpRequest) -> func.HttpResponse:
  
    logging.info("HTTP trigger received. Starting pipeline...")
    logging.info("--- 1. EXTRACTING DATA FROM API ---")
        # --- 1. EXTRACT ---
    api_url = "https://restful-api.dev/" # !! REPLACE
    response = requests.get(api_url)
    response.raise_for_status()
    raw_data = response.json()
    logging.info("Successfully fetched data from API.")

    logging.info("--- 2. TRANSFORMING DATA WITH PANDAS ---")
        # --- 2. TRANSFORM ---
    df = pd.json_normalize(raw_data)
        # 1. Clean column names
    df.columns = df.columns.str.replace(' ', '_').str.upper()

        # 2. Fix for duplicate columns
    logging.info("Checking for and renaming duplicate columns...")
    new_cols = []
    counts = {}
    for col in df.columns:
        if col in counts:
            counts[col] += 1
            new_cols.append(f"{col}_{counts[col]-1}")
        else:
            counts[col] = 1
            new_cols.append(col)
        df.columns = new_cols
        logging.info("--- 3. LOADING DATA TO SNOWFLAKE ---")
        # --- 3. LOAD ---
        conn = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse="PYTHON_ETL_WH",
            database="MY_API_PROJECT_DB",
            schema="RAW_DATA"
        )
        logging.info("Successfully connected to Snowflake.")
        write_pandas(conn=conn, df=df, table_name="MY_API_DATA", overwrite=True)
        logging.info("Success! Data was written to Snowflake table.")
        conn.close()

    # 3. Send a "success" response back to your browser
    return func.HttpResponse("Pipeline finished successfully.", status_code=200)