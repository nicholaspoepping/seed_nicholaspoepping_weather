import requests
import pandas as pd
from datetime import datetime

# --- CONFIGURATION ---
API_KEY = "YOUR_API_KEY_HERE" # Replace with your actual key
FILE_NAME = "US_AGGREGATE_NATGAS.csv"

# Weighted Basket for Natural Gas Demand
# Weights approx. based on residential gas consumption impact
LOCATIONS = [
    {"name": "Chicago", "lat": "41.8781", "lon": "-87.6298", "weight": 0.35},
    {"name": "New York", "lat": "40.7128", "lon": "-74.0060", "weight": 0.30},
    {"name": "Denver",   "lat": "39.7392", "lon": "-104.9903", "weight": 0.15},
    {"name": "Houston",  "lat": "29.7604", "lon": "-95.3698",  "weight": 0.10},
    {"name": "Atlanta",  "lat": "33.7490", "lon": "-84.3880",  "weight": 0.10}
]

def fetch_forecast(lat, lon):
    url = f"https://api.tomorrow.io/v4/weather/forecast?location={lat},{lon}&apikey={API_KEY}"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error fetching {lat},{lon}: {response.text}")
        return []
    return response.json()['timelines']['daily']

def run_aggregate():
    print("Fetching weather for 5 key hubs...")
    
    # We need to align dates across all cities. 
    # Dictionary structure: { '2026-02-05': { 'temp_weighted_sum': 0, 'total_weight': 0 } }
    date_map = {}

    for loc in LOCATIONS:
        print(f"Querying {loc['name']}...")
        daily_data = fetch_forecast(loc['lat'], loc['lon'])
        
        for day in daily_data:
            dt = day['time'] # ISO String
            vals = day['values']
            avg_temp = vals.get('temperatureAvg', 0)
            
            if dt not in date_map:
                date_map[dt] = {'weighted_temp': 0, 'accumulated_weight': 0}
            
            # Add weighted contribution
            date_map[dt]['weighted_temp'] += (avg_temp * loc['weight'])
            date_map[dt]['accumulated_weight'] += loc['weight']

    # --- PROCESS & FORMAT ---
    csv_rows = []
    sorted_dates = sorted(date_map.keys())

    for dt in sorted_dates:
        data = date_map[dt]
        
        # Normalize (in case a city failed and weight < 1.0)
        final_avg_temp = data['weighted_temp'] / data['accumulated_weight']
        
        # CALCULATE HDD (Heating Degree Days)
        # Standard base is 65°F (approx 18.3°C). 
        # If temp is 10°C, HDD = 8.3. If temp is 20°C, HDD = 0.
        # Tomorrow.io is usually Celsius by default.
        base_temp_c = 18.33
        hdd = max(0, base_temp_c - final_avg_temp)
        cdd = max(0, final_avg_temp - base_temp_c) # Cooling Degree Days (Summer demand)

        row = {
            'time': dt,
            'open': final_avg_temp,  # Open = Raw Avg Temp
            'high': final_avg_temp + 2, # Synthetic range for visibility
            'low': final_avg_temp - 2,
            'close': hdd,            # CLOSE = HDD (The most important metric!)
            'volume': int(cdd * 10)  # Volume = CDD (for summer tracking)
        }
        csv_rows.append(row)

    df = pd.DataFrame(csv_rows)
    df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
    
    # Save
    df.to_csv(FILE_NAME, index=False)
    print(f"Successfully created Aggregate US Forecast: {FILE_NAME}")

if __name__ == "__main__":
    run_aggregate()
