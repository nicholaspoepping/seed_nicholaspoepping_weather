import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# --- CONFIGURATION ---
API_KEY = os.environ.get("TOMORROW_API_KEY")
# CHANGED: Started from 2021 to capture the historic Volatility of that winter
HISTORY_START_YEAR = 2021 

LOCATIONS = [
    {"name": "Chicago", "lat": 41.8781, "lon": -87.6298, "weight": 0.35},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060, "weight": 0.30},
    {"name": "Denver",   "lat": 39.7392, "lon": -104.9903", "weight": 0.15},
    {"name": "Houston",  "lat": 29.7604, "lon": -95.3698,  "weight": 0.10},
    {"name": "Atlanta",  "lat": 33.7490", "lon": -84.3880,  "weight": 0.10}
]

def fetch_data():
    print("--- Starting Data Fetch (5-Year Scope) ---")
    
    # 1. FETCH HISTORY (Open-Meteo)
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = f"{HISTORY_START_YEAR}-01-01"
    
    daily_hist = pd.DataFrame()
    try:
        hist_frames = []
        for loc in LOCATIONS:
            # Using Open-Meteo for deep history (Free & Fast)
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
                    'time': data['daily']['time'],
                    'temp': data['daily']['temperature_2m_mean']
                })
                df['weight'] = loc['weight']
                hist_frames.append(df)
        
        if hist_frames:
            full_hist = pd.concat(hist_frames)
            full_hist['weighted_temp'] = full_hist['temp'] * full_hist['weight']
            daily_hist = full_hist.groupby('time')['weighted_temp'].sum().reset_index()
            daily_hist.rename(columns={'weighted_temp': 'avg_temp'}, inplace=True)
            print(f"History fetched: {len(daily_hist)} days ({HISTORY_START_YEAR}-Present).")
    except Exception as e:
        print(f"History Error: {e}")

    # 2. FETCH FORECAST (Tomorrow.io)
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
                        dt = day['time'].split('T')[0]
                        temp = day['values'].get('temperatureAvg', 0)
                        fore_map[dt] = fore_map.get(dt, 0) + (temp * loc['weight'])
            
            if fore_map:
                daily_fore = pd.DataFrame(list(fore_map.items()), columns=['time', 'avg_temp'])
                print(f"Forecast fetched: {len(daily_fore)} days.")
        except Exception as e:
            print(f"Forecast Error: {e}")

    # 3. MERGE
    df_final = pd.concat([daily_hist, daily_fore]) if not daily_hist.empty else daily_fore
    if not df_final.empty:
        df_final = df_final.drop_duplicates(subset='time', keep='last').sort_values('time')
    
    return df_final

def generate_files(df):
    if df.empty: return

    # CHANGED: Removed the .tail(365) limit. 
    # We now output the FULL dataset (2021-2026).
    
    hdds = []
    dates = []
    
    for _, row in df.iterrows():
        hdd = max(0, 18.33 - row['avg_temp'])
        dt_obj = datetime.strptime(row['time'], "%Y-%m-%d")
        unix_ms = int(dt_obj.timestamp() * 1000)
        
        # Rounding to save character space in the text file
        hdds.append(str(round(hdd, 2)))
        dates.append(str(unix_ms))

    # We format the array string carefully to ensure it fits valid Pine syntax
    pine_content = f"""// --- PASTE INTO PINE EDITOR ---
// Data Range: {HISTORY_START_YEAR} to {datetime.now().strftime('%Y-%m-%d')}
// Context: 5-Year History + 14-Day Forecast
var float[] hdd_data = array.from({', '.join(hdds)})
var int[] time_data = array.from({', '.join(dates)})

// --- RENDERING LOGIC ---
var float current_hdd = na
int bar_time = time
int array_len = array.size(time_data)
int ms_in_day = 86400000

// 1. Historical Alignment
for i = 0 to array_len - 1
    int t_data = array.get(time_data, i)
    if math.abs(bar_time - t_data) < 43200000
        current_hdd := array.get(hdd_data, i)
        break

plot(current_hdd, title="Historical HDD", color=color.blue, style=plot.style_columns, linewidth=2)

// 2. Future Projection
if barstate.islast
    for i = 0 to array_len - 1
        int t_data = array.get(time_data, i)
        float hdd_val = array.get(hdd_data, i)
        if t_data > bar_time
            int bars_forward = math.round((t_data - bar_time) / ms_in_day)
            if bars_forward > 0
                box.new(left=bar_index + bars_forward, top=hdd_val, bottom=0, right=bar_index + bars_forward, 
                     bgcolor=color.new(color.orange, 20), border_color=color.orange, border_width=2)
// --- END PASTE ---
"""
    
    with open("pine_code.txt", "w") as f:
        f.write(pine_content)
    print("Generated pine_code.txt with full history")

if __name__ == "__main__":
    df = fetch_data()
    generate_files(df)
