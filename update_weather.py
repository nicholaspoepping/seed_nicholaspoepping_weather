import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# --- CONFIGURATION ---
API_KEY = os.environ.get("TOMORROW_API_KEY")
HISTORY_START_YEAR = 2024 

LOCATIONS = [
    {"name": "Chicago", "lat": 41.8781, "lon": -87.6298, "weight": 0.35},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060, "weight": 0.30},
    {"name": "Denver",   "lat": 39.7392, "lon": -104.9903, "weight": 0.15},
    {"name": "Houston",  "lat": 29.7604, "lon": -95.3698,  "weight": 0.10},
    {"name": "Atlanta",  "lat": 33.7490", "lon": -84.3880,  "weight": 0.10}
]

def fetch_data():
    print("--- Starting Data Fetch ---")
    
    # 1. FETCH HISTORY (Open-Meteo)
    print("Fetching History...")
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = f"{HISTORY_START_YEAR}-01-01"
    
    daily_hist = pd.DataFrame()
    
    try:
        hist_frames = []
        for loc in LOCATIONS:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": loc['lat'], 
                "longitude": loc['lon'], 
                "start_date": start_date, 
                "end_date": end_date, 
                "daily": "temperature_2m_mean"
            }
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            
            if 'daily' in data:
                df = pd.DataFrame({
                    'time': data['daily']['time'], # Format is YYYY-MM-DD
                    'temp': data['daily']['temperature_2m_mean']
                })
                df['weight'] = loc['weight']
                hist_frames.append(df)
        
        if hist_frames:
            full_hist = pd.concat(hist_frames)
            # Weighted Average Calculation
            full_hist['weighted_temp'] = full_hist['temp'] * full_hist['weight']
            daily_hist = full_hist.groupby('time')['weighted_temp'].sum().reset_index()
            daily_hist.rename(columns={'weighted_temp': 'avg_temp'}, inplace=True)
            print(f"History fetched: {len(daily_hist)} days.")
    except Exception as e:
        print(f"History Fetch Error: {e}")

    # 2. FETCH FORECAST (Tomorrow.io)
    print("Fetching Forecast...")
    daily_fore = pd.DataFrame()
    
    if API_KEY:
        try:
            fore_map = {}
            for loc in LOCATIONS:
                url = f"https://api.tomorrow.io/v4/weather/forecast?location={loc['lat']},{loc['lon']}&apikey={API_KEY}"
                r = requests.get(url, headers={"accept": "application/json"}, timeout=10)
                if r.status_code == 200:
                    timelines = r.json().get('timelines', {}).get('daily', [])
                    for day in timelines:
                        # CRITICAL FIX: Normalize time to YYYY-MM-DD to avoid duplicates
                        dt = day['time'].split('T')[0]
                        temp = day['values'].get('temperatureAvg', 0)
                        
                        if dt not in fore_map:
                            fore_map[dt] = 0
                        fore_map[dt] += (temp * loc['weight'])
            
            if fore_map:
                daily_fore = pd.DataFrame(list(fore_map.items()), columns=['time', 'avg_temp'])
                print(f"Forecast fetched: {len(daily_fore)} days.")
        except Exception as e:
            print(f"Forecast Fetch Error: {e}")

    # 3. MERGE & CLEAN
    df_final = pd.DataFrame()
    if not daily_hist.empty and not daily_fore.empty:
        df_final = pd.concat([daily_hist, daily_fore])
    elif not daily_hist.empty:
        df_final = daily_hist
    elif not daily_fore.empty:
        df_final = daily_fore

    if not df_final.empty:
        # STRICT DEDUPLICATION: This line fixes the CSV error
        df_final = df_final.drop_duplicates(subset='time', keep='last')
        df_final = df_final.sort_values('time')
    
    return df_final

def generate_files(df):
    if df.empty:
        print("No data to write.")
        # Create empty dummy files so Git doesn't crash
        with open("pine_code.txt", "w") as f: f.write("// No Data")
        return

    # A. Generate CSV (For Seed)
    csv_df = df.copy()
    csv_df['open'] = csv_df['avg_temp']
    csv_df['high'] = csv_df['avg_temp'] + 2
    csv_df['low'] = csv_df['avg_temp'] - 2
    csv_df['close'] = csv_df['avg_temp'].apply(lambda x: max(0, 18.33 - x)) # HDD
    csv_df['volume'] = 0
    
    # Fix Time Format for TradingView CSV (Must be ISO)
    csv_df['time'] = csv_df['time'].apply(lambda x: f"{x}T00:00:00Z")
    
    csv_df = csv_df[['time', 'open', 'high', 'low', 'close', 'volume']]
    csv_df.to_csv("US_AGGREGATE_NATGAS.csv", index=False)
    print("Generated US_AGGREGATE_NATGAS.csv")

    # B. Generate Pine Script Text (For Manual Paste)
    df_recent = df.tail(365) # Keep last 365 days
    hdds = []
    dates = []
    
    for _, row in df_recent.iterrows():
        hdd = max(0, 18.33 - row['avg_temp'])
        # Convert YYYY-MM-DD to Unix Time (ms)
        dt_obj = datetime.strptime(row['time'], "%Y-%m-%d")
        unix_ms = int(dt_obj.timestamp() * 1000)
        hdds.append(str(round(hdd, 2)))
        dates.append(str(unix_ms))

    pine_content = f"""// --- PASTE INTO PINE EDITOR ---
// Last Update: {datetime.now().strftime('%Y-%m-%d')}
var float[] hdd_data = array.from({', '.join(hdds)})
var int[] time_data = array.from({', '.join(dates)})

var float current_hdd = na
int time_ms = time
int array_size = array.size(time_data)

for i = 0 to array_size - 1
    if array.get(time_data, i) == time_ms
        current_hdd := array.get(hdd_data, i)
        break

plot(current_hdd, title="HDD Forecast", color=color.blue, style=plot.style_columns, linewidth=2)
// --- END PASTE ---
"""
    
    with open("pine_code.txt", "w") as f:
        f.write(pine_content)
    print("Generated pine_code.txt")

if __name__ == "__main__":
    df = fetch_data()
    generate_files(df)
