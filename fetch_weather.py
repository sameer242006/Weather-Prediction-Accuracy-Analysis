import json
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from io import StringIO


# Load API key and MySQL credentials from JSON
with open('config/credentials.json') as f:
    creds = json.load(f)

API_KEY = creds['api_key']
mysql_config = creds['mysql']

# MySQL connection string
connection_string = f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
engine = create_engine(connection_string)

# Cities
cities = [
"Mumbai,IN", "Delhi,IN", "Pune,IN", "Chennai,IN", "bengaluru,IN",
    "Ahmedabad,IN", "Kolkata,IN", "Hyderabad,IN", "Jaipur,IN", "Lucknow,IN"
]
# Change this to desired month range
start_date = "2025-10-5"
end_date = "2025-11-24"


# Loop through each city
for city in cities:
    city_slug = city.lower().replace(",", "_").replace(" ", "_")
    print(f"📦 Fetching data for {city} from {start_date} to {end_date}...")

    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{start_date}/{end_date}?unitGroup=metric&key={API_KEY}&include=days&contentType=csv"

    try:
        response = requests.get(url)
        response.raise_for_status()

        # Read CSV response
        df = pd.read_csv(StringIO(response.text))
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Convert date column to datetime
        df['datetime'] = pd.to_datetime(df['datetime'])

        # Remove existing data for this month to avoid duplicates
        with engine.begin() as conn:
            delete_query = text(f"""
                DELETE FROM {city_slug}
                WHERE datetime BETWEEN :start AND :end
            """)
            conn.execute(delete_query, {
                'start': start_date,
                'end': end_date
            })
            print(f"🗑️ Old data for {city} in range {start_date} to {end_date} deleted.")

        # Append new data
        df.to_sql(city_slug, con=engine, if_exists='append', index=False)
        print(f"✅ Data for {city} added to `{city_slug}` table.\n")

    except Exception as e:
        print(f"❌ Failed to fetch/store data for {city}: {e}\n")
