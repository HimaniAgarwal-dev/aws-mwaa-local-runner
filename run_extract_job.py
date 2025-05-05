from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
import json
import subprocess

# Replace these with your actual Snowflake credentials
SNOWFLAKE_CONFIG = {
    'user': 'HimaniAgarwal',
    'password': 'Snowpass@$2025,
    'account': 'UZVLGEQ_GOB74346.snowflakecomputing.com',
    'warehouse': 'COMPUTE_WH',
    'database': 'streamlit_app',
    'schema': 'raw'
}

# Get scheduled preferences from Snowflake
def get_scheduled_preferences(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT preferences
        FROM extract_preferences
        WHERE preferences:enable_schedule::boolean = TRUE
    """)
    return cursor.fetchall()

# Check if it's time to run
def should_run_now(schedule):
    now = datetime.utcnow()
    freq = schedule.get("frequency")
    time_of_day = schedule.get("time_of_day")  # e.g., "06:00"

    if not freq or not time_of_day:
        return False

    scheduled_time = datetime.strptime(time_of_day, "%H:%M").time()
    return now.time().hour == scheduled_time.hour and now.time().minute == scheduled_time.minute

# Main logic
def main():
    conn = get_snowflake_connection()
    rows = get_scheduled_preferences(conn)

    for (pref_json,) in rows:
        preferences = json.loads(pref_json)
        schedule = preferences.get("schedule", {})
        if should_run_now(schedule):
            print("✅ Running job for config:")
            print(json.dumps(preferences, indent=2))
        else:
            print("⏳ Not time yet for:")
            print(json.dumps(preferences.get("extract_name", {})))

if __name__ == "__main__":
    main()
