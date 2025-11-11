import azure.functions as func
import logging
import os  # Added 'os' to read the local settings
 
# --- Your original imports ---
import requests
import snowflake.connector
import sys
# 'time' is no longer needed, as the timer trigger handles the schedule
 
# --- Your original global variable (This is good) ---
INSERT_SQL = """
INSERT INTO RAW_USER_SIGNUPS
    (first_name, last_name, email, country, signup_time)
VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())
"""
 
# --- Your original 'fetch' function (NO CHANGES) ---
# This is preserved 100%
def fetch_random_user():
    """Calls the live API and parses the response."""
    try:
        response = requests.get("https://randomuser.me/api/")
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()["results"][0]
        first = data["name"]["first"]
        last = data["name"]["last"]
        email = data["email"]
        country = data["location"]["country"]
        # We use logging.info instead of print for better Azure logs
        logging.info(f"Fetched new user: {first} {last} from {country}")
        return (first, last, email, country)
    except requests.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None
 
# --- Your original 'insert' function (MINOR CHANGE) ---
# We now pass SNOWFLAKE_CONFIG in as an argument,
# instead of reading it as a global variable.
def insert_data_to_snowflake(data, SNOWFLAKE_CONFIG): 
    """Connects to Snowflake and inserts one row."""
    try:
        with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, data)
                logging.info(" -> Successfully inserted into Snowflake.")
    except snowflake.connector.Error as e:
        logging.error(f"Error inserting data into Snowflake: {e}")
 
# --- NEW: The Azure Function Entry Point ---
# This 'main' function REPLACES your 'if __name__ == "__main__"' block
def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function ran.')
 
    # 1. Build the config dictionary by reading from local.settings.json
    try:
        SNOWFLAKE_CONFIG = {
            "user": os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
            "database": os.environ["SNOWFLAKE_DATABASE"],
            "schema": os.environ["SNOWFLAKE_SCHEMA"],
            "role": os.environ["SNOWFLAKE_ROLE"]
        }
    except KeyError as e:
        logging.error(f"Missing setting: {e}. Check your local.settings.json.")
        return
 
    # 2. This is your core logic, taken from your 'for' loop
    user_data = fetch_random_user()
    if user_data:
        # 3. We pass the config to your function
        insert_data_to_snowflake(user_data, SNOWFLAKE_CONFIG)
 
    logging.info("Function run finished.")