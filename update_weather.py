import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# --- CONFIGURATION ---
API_KEY = os.environ.get("TOMORROW_API_KEY")
HISTORY_START_YEAR = 2024 # Keep it shorter for hardcoded arrays (Script Char Limit)
LOCATIONS = [
    {"name": "Chicago", "lat": 41.8781, "lon": -87.6298, "weight": 0.35},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060, "weight": 0.30},
    {"name": "Denver",   "lat": 39.7392, "lon": -104.9903, "weight": 0.15},
    {"name": "Houston",  "lat": 29.7604, "lon": -95.3698,  "weight": 0.10},
    {"name": "Atlanta",  "lat": 33.7490", "lon": -84.3880,  "weight": 0.10}
]

def fetch_history_and_forecast():
    # 1. Fetch History (Open-Meteo)
    print("Fetching History...")
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = f"{HISTORY_START_YEAR}-01-01"
    
    history_frames = []
    for loc in LOCATIONS:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {"latitude": loc['lat'], "longitude": loc['lon'], "start_date": start_date, "end_date": end_date, "daily": "temperature_2m_mean"}
        r = requests.get(url, params=params).json()
        df = pd.DataFrame({'time': r['daily']['time'], 'temp': r['daily']['temperature_2m_mean']})
        df['weight'] = loc['weight']
        history_frames.append(df)
    
    full_hist = pd.concat(history_frames)
    daily_hist = full_hist.groupby('time').apply(lambda x: (x['temp'] * x['weight']).sum()).reset_index(name='avg_temp')

    # 2. Fetch Forecast (Tomorrow.io)
    print("Fetching Forecast...")
    forecast_map = {}
    if API_KEY:
        for loc in LOCATIONS:
            url = f"https://api.tomorrow.io/v4/weather/forecast?location={loc['lat']},{loc['lon']}&apikey={API_KEY}"
            try:
                data = requests.get(url).json()['timelines']['daily']
                for d in data:
                    dt = d['time'].split('T')[0]
                    val = d['values']['temperatureAvg']
                    forecast_map[dt] = forecast_map.get(dt, 0) + (val * loc['weight'])
            except: pass
    
    daily_fore = pd.DataFrame(list(forecast_map.items()), columns=['time', 'avg_temp'])
    
    # 3. Merge
    df_final = pd.concat([daily_hist, daily_fore]).drop_duplicates(subset='time', keep='last').sort_values('time')
    
    return df_final

def generate_pine_code(df):
    # Convert to HDD
    base_temp = 18.33
    hdds = []
    dates = []
    
    for _, row in df.iterrows():
        hdd = max(0, base_temp - row['avg_temp'])
        # Convert date to Unix Timestamp (ms) for Pine
        dt_obj = datetime.strptime(row['time'], '%Y-%m-%d')
        unix_time = int(dt_obj.timestamp() * 1000)
        
        hdds.append(str(round(hdd, 2)))
        dates.append(str(unix_time))

    # SPLIT DATA into chunks (Pine Script has a char limit per line)
    # We will just take the last 365 days to be safe and efficient
    hdds = hdds[-365:]
    dates = dates[-365:]

    pine_script = f"""
// AUTO-GENERATED PINE SCRIPT
// COPY BELOW THIS LINE
var float[] hdd_data = array.from({', '.join(hdds)})
var int[] time_data = array.from({', '.join(dates)})

// Align data to chart
var float current_hdd = na
int time_ms = time
int array_size = array.size(time_data)

// Simple lookup (O(N) - optimized for recent data)
for i = 0 to array_size - 1
    if array.get(time_data, i) == time_ms
        current_hdd := array.get(hdd_data, i)
        break

plot(current_hdd, title="HDD Forecast", color=color.blue, style=plot.style_columns, linewidth=2)
// COPY ABOVE THIS LINE
"""
    
    with open("pine_code.txt", "w") as f:
        f.write(pine_script)
    print("Successfully generated pine_code.txt")

if __name__ == "__main__":
    df = fetch_history_and_forecast()
    generate_pine_code(df)
