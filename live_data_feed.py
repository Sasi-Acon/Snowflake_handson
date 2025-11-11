import requests 
import snowflake.connector
import time
import sys
# --- CONFIGURE YOUR SNOWFLAKE CONNECTION HERE ---
SNOWFLAKE_CONFIG = {
    "user": "SASIDARAN1CA",
    "password": "Sriram.ca@03041998",
    "account": "YAJPVIR-JX75342", 
    "warehouse": "COMPUTE_WH",
    "database": "python_api_insert_test",
    "schema": "PUBLIC"}
# SQL command to insert our data
INSERT_SQL = """
INSERT INTO RAW_USER_SIGNUPS 
    (first_name, last_name, email, country, signup_time) 
VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP())
"""
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
        
        print(f"Fetched new user: {first} {last} from {country}")
        return (first, last, email, country)
        
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}", file=sys.stderr)
        return None
    
def insert_data_to_snowflake(data):
    """Connects to Snowflake and inserts one row."""
    try:
        with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, data)
                print(" -> Successfully inserted into Snowflake.")
                
    except snowflake.connector.Error as e:
        print(f"Error inserting data into Snowflake: {e}", file=sys.stderr)

# --- Main loop ---
if __name__ == "__main__":
    print("Starting live data feed... Press CTRL+C to stop.")
    
    # We'll run it 10 times for this test
    for i in range(10):
        user_data = fetch_random_user()
        
        if user_data:
            insert_data_to_snowflake(user_data)
            
        # Wait for 10 seconds before fetching the next user
        time.sleep(10)
    
    print("Data feed simulation finished.")
 