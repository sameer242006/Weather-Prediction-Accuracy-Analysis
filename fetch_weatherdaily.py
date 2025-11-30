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
connection_string = (
    f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}"
    f"@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
)
engine = create_engine(connection_string)

# Cities
cities = [
    "Mumbai,IN", "Delhi,IN", "Pune,IN", "Chennai,IN", "Bengaluru,IN",
    "Ahmedabad,IN", "Kolkata,IN", "Hyderabad,IN", "Jaipur,IN", "Lucknow,IN"
]

# ✅ Use today's date
today = datetime.today().strftime("%Y-%m-%d")
start_date = today
end_date = today

# Loop through each city
for city in cities:
    city_slug = city.lower().replace(",", "_").replace(" ", "_")
    print(f"📦 Fetching actual weather for {city} on {today}...")

    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{city}/{start_date}/{end_date}?unitGroup=metric&key={API_KEY}&include=days&contentType=csv"
    )

    try:
        response = requests.get(url)
        response.raise_for_status()

        # Read CSV response
        df = pd.read_csv(StringIO(response.text))
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Convert date column to datetime
        df['datetime'] = pd.to_datetime(df['datetime']).dt.date

        # Keep only useful columns
        df_clean = df[['datetime', 'temp', 'humidity', 'windspeed', 'precip', 'conditions']].copy()

        # Remove existing data for today to avoid duplicates
        with engine.begin() as conn:
            delete_query = text(f"""
                DELETE FROM {city_slug}
                WHERE datetime = :today
            """)
            conn.execute(delete_query, {'today': today})
            print(f"🗑️ Old data for {city} on {today} deleted.")

        # Append new data
        df_clean.to_sql(city_slug, con=engine, if_exists='append', index=False)
        print(f"✅ Actual weather for {city} added to `{city_slug}` table.\n")

    except Exception as e:
        print(f"❌ Failed to fetch/store data for {city}: {e}\n")
