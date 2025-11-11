import requests
import pandas as pd
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas # The key function
 
print("--- 1. EXTRACTING DATA FROM API ---")
 
# --- 1. EXTRACT ---
# The URL of the API you want to get data from
api_url = "https://api.restful-api.dev/objects"  # !! REPLACE with your API's URL
 
# Some APIs require an API key or other "headers" to know who you are.
headers = {"content-type": "application/json"}
#payload = json.dumps({ "name": "Apple AirPods", "data": { "color": "white", "generation": "3rd", "price": 135}})
#requestUrl = "https://api.restful-api.dev/objects/6"
#r = requests.put(requestUrl, data=payload, headers=headers)

# Make the "GET" request to the API
try:
    response = requests.get(api_url, headers=headers)
    # This line will check if the request failed (status code 4xx or 5xx)
    response.raise_for_status() 
    # Get the data from the response and turn it into a Python object (a dict or list)
    raw_data = response.json()
    print("Successfully fetched data from API.")
 
except requests.exceptions.RequestException as e:
    print(f"Failed to get data from API: {e}")
    exit() # Stop the script if the API call fails
 
 
print("\n--- 2. TRANSFORMING DATA WITH PANDAS ---")
 
# --- 2. TRANSFORM ---
try:
    # pd.json_normalize is the best way to "flatten" nested JSON from an API.
    # Common case: The data is a list of records inside a top-level key
    # e.g., {"count": 50, "results": [ ...your data... ]}
    # df = pd.json_normalize(raw_data, record_path=['results']) 
    # Other case: The data is just a list of records
    # e.g., [ {"id": 1, "name": "A"}, {"id": 2, "name": "B"} ]
    df = pd.json_normalize(raw_data)
    # If the above lines fail, your JSON structure might be different.
    # You can print raw_data to inspect it:
    # print(raw_data)
 
    print(f"Successfully loaded data into a table with {len(df)} rows.")
    print("Data Head (first 5 rows):")
    print(df.head())
 
except Exception as e:
    print(f"Failed to normalize JSON data. Error: {e}")
    print("Printing the raw data to help you debug:")
    print(raw_data)
    exit() # Stop the script if transformation fails
 
 
print("\n--- 3. LOADING DATA TO SNOWFLAKE ---")
 
# --- 3. LOAD ---
try:
    # !! REPLACE all of these with your real credentials !!
    conn = snowflake.connector.connect(
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        warehouse="PYTHON_ETL_WH",
        database="MY_API_PROJECT_DB",
        schema="RAW_DATA"
    )
 
    print("Successfully connected to Snowflake.")
 
    # IMPORTANT: Snowflake table names are often uppercase.
    # We use uppercase for the table name and for the column names.
    table_name = "MY_API_DATA"
    # Clean column names for Snowflake (replaces spaces with _, makes uppercase)
    df.columns = df.columns.str.replace(' ', '_').str.upper()
    print(list(df.columns))
    # --- FIX FOR DUPLICATE COLUMNS ---
    print("Checking for and renaming duplicate columns...")
    new_cols = []
    counts = {}
    
    for col in df.columns:
        if col in counts:
            print("dup name "+col)
            counts[col] += 1
            # Add a suffix like _1, _2 to the duplicate
            new_cols.append(f"{col}_{counts[col]-1}") 
        else:
            print("dup name "+col)
            counts[col] = 1
            new_cols.append(col)
    
    df.columns = new_cols
    # ---- END OF FIX ----

    # This is the command that does all the work.
    write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        auto_create_table=True,  # Creates the table if it doesn't exist
        overwrite=True           # Replaces the table every time it runs
    )
 
    print(f"Success! Data was written to Snowflake table: {table_name}")
 
except snowflake.connector.Error as e:
    print(f"Snowflake Error: {e}")
 
finally:
    # Always close the connection
    if 'conn' in locals():
        conn.close()
        print("Snowflake connection closed.")