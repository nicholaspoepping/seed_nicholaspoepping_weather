import requests
import pandas as pd
import os
import sys

# --- CONFIGURATION ---
# We fetch the API key from GitHub Secrets for security
API_KEY = os.environ.get("TOMORROW_API_KEY")
FILE_NAME = "US_AGGREGATE_NATGAS.csv"

# Weighted Basket for Natural Gas Demand
LOCATIONS = [
    {"name": "Chicago", "lat": "41.8781", "lon": "-87.6298", "weight": 0.35},
    {"name": "New York", "lat": "40.7128", "lon": "-74.0060", "weight": 0.30},
    {"name": "Denver",   "lat": "39.7392", "lon": "-104.9903", "weight": 0.15},
    {"name": "Houston",  "lat": "29.7604", "lon": "-95.3698",  "weight": 0.10},
    {"name": "Atlanta",  "lat": "33.7490", "lon": "-84.3880",  "weight": 0.10}
]

def fetch_forecast(lat, lon):
    if not API_KEY:
        print("ERROR: API Key not found. Make sure TOMORROW_API_KEY is set in Secrets.")
        sys.exit(1)
        
    url = f"https://api.tomorrow.io/v4/weather/forecast?location={lat},{lon}&apikey={API_KEY}"
    headers = {"accept": "application/json"}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()['timelines']['daily']
    except Exception as e:
        print(f"Error fetching {lat},{lon}: {e}")
        return []

def run_aggregate():
    print(f"Starting Weather Aggregate Job for {len(LOCATIONS)} cities...")
    date_map = {}

    for loc in LOCATIONS:
        print(f"Querying {loc['name']}...")
        daily_data = fetch_forecast(loc['lat'], loc['lon'])
        
        for day in daily_data:
            dt = day['time']
            vals = day['values']
            avg_temp = vals.get('temperatureAvg', 0)
            
            if dt not in date_map:
                date_map[dt] = {'weighted_temp': 0, 'accumulated_weight': 0}
            
            date_map[dt]['weighted_temp'] += (avg_temp * loc['weight'])
            date_map[dt]['accumulated_weight'] += loc['weight']

    # --- PROCESS & FORMAT ---
    csv_rows = []
    sorted_dates = sorted(date_map.keys())

    for dt in sorted_dates:
        data = date_map[dt]
        if data['accumulated_weight'] == 0: continue

        final_avg_temp = data['weighted_temp'] / data['accumulated_weight']
        
        # HDD Calculation (Base 65F / 18.33C)
        base_temp_c = 18.33
        hdd = max(0, base_temp_c - final_avg_temp)
        cdd = max(0, final_avg_temp - base_temp_c)

        row = {
            'time': dt,
            'open': round(final_avg_temp, 2),
            'high': round(final_avg_temp + 2, 2), # Synthetic High
            'low': round(final_avg_temp - 2, 2),  # Synthetic Low
            'close': round(hdd, 2),               # CLOSE = HDD
            'volume': int(cdd * 10)               # VOLUME = CDD
        }
        csv_rows.append(row)

    if not csv_rows:
        print("No data collected. Exiting.")
        return

    df = pd.DataFrame(csv_rows)
    df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
    
    df.to_csv(FILE_NAME, index=False)
    print(f"SUCCESS: Generated {FILE_NAME} with {len(df)} rows.")

if __name__ == "__main__":
    run_aggregate()
