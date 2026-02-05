import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import sys

# --- CONFIGURATION ---
API_KEY = os.environ.get("TOMORROW_API_KEY")
FILE_NAME = "US_AGGREGATE_NATGAS.csv"
HISTORY_START_YEAR = 2020

# The "NatG 5" Weighted Basket
LOCATIONS = [
    {"name": "Chicago", "lat": 41.8781, "lon": -87.6298, "weight": 0.35},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060, "weight": 0.30},
    {"name": "Denver",   "lat": 39.7392, "lon": -104.9903, "weight": 0.15},
    {"name": "Houston",  "lat": 29.7604, "lon": -95.3698,  "weight": 0.10},
    {"name": "Atlanta",  "lat": 33.7490, "lon": -84.3880,  "weight": 0.10}
]

# --- PART 1: FETCH HISTORY (Open-Meteo - Free) ---
def fetch_history_data():
    print("Fetching Historical Data (Open-Meteo)...")
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = f"{HISTORY_START_YEAR}-01-01"
    
    # We fetch all cities in one go if possible, but loop is safer for clarity
    history_frames = []
    
    for loc in LOCATIONS:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": loc['lat'],
            "longitude": loc['lon'],
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_mean",
            "timezone": "UTC"
        }
        r = requests.get(url, params=params)
        data = r.json()
        
        # Create DataFrame for this city
        df = pd.DataFrame({
            'time': data['daily']['time'],
            'temp': data['daily']['temperature_2m_mean']
        })
        df['weight'] = loc['weight']
        history_frames.append(df)
        
    # Combine and Calculate Weighted Avg for History
    full_history = pd.concat(history_frames)
    # Group by date and sum (Temp * Weight)
    # Note: We need to normalize if any data is missing, but Open-Meteo is reliable.
    full_history['weighted_temp'] = full_history['temp'] * full_history['weight']
    
    daily_history = full_history.groupby('time')['weighted_temp'].sum().reset_index()
    daily_history.rename(columns={'weighted_temp': 'avg_temp'}, inplace=True)
    return daily_history

# --- PART 2: FETCH FORECAST (Tomorrow.io - Premium) ---
def fetch_forecast_data():
    if not API_KEY:
        print("Skipping Forecast (No API Key)")
        return pd.DataFrame()

    print("Fetching Forecast Data (Tomorrow.io)...")
    date_map = {}
    
    for loc in LOCATIONS:
        url = f"https://api.tomorrow.io/v4/weather/forecast?location={loc['lat']},{loc['lon']}&apikey={API_KEY}"
        headers = {"accept": "application/json"}
        try:
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            daily = r.json()['timelines']['daily']
            
            for day in daily:
                dt = day['time'].split('T')[0] # Keep strictly YYYY-MM-DD
                temp = day['values'].get('temperatureAvg', 0)
                
                if dt not in date_map:
                    date_map[dt] = 0
                date_map[dt] += (temp * loc['weight'])
                
        except Exception as e:
            print(f"Forecast Error for {loc['name']}: {e}")

    # Convert to DataFrame
    forecast_rows = [{'time': k, 'avg_temp': v} for k, v in date_map.items()]
    return pd.DataFrame(forecast_rows)

# --- MAIN EXECUTION ---
def run_full_cycle():
    # 1. Get History
    df_hist = fetch_history_data()
    
    # 2. Get Forecast
    df_fore = fetch_forecast_data()
    
    # 3. Merge (Forecast overrides History if dates overlap)
    # Ensure both have 'time' as datetime for sorting
    if not df_hist.empty:
        df_hist['time'] = pd.to_datetime(df_hist['time'])
    if not df_fore.empty:
        df_fore['time'] = pd.to_datetime(df_fore['time'])
        
    # Concatenate
    df_final = pd.concat([df_hist, df_fore]).drop_duplicates(subset='time', keep='last')
    df_final.sort_values('time', inplace=True)
    
    # 4. Calculate HDD/CDD and Format
    output_rows = []
    base_temp_c = 18.33 # 65F
    
    for _, row in df_final.iterrows():
        t = row['avg_temp']
        hdd = max(0, base_temp_c - t)
        cdd = max(0, t - base_temp_c)
        
        output_rows.append({
            'time': row['time'].strftime('%Y-%m-%dT%H:%M:%SZ'), # TradingView Format
            'open': round(t, 2),
            'high': round(t + 2, 2),
            'low': round(t - 2, 2),
            'close': round(hdd, 2), # HDD is key
            'volume': int(cdd * 10)
        })
        
    # Save
    final_df = pd.DataFrame(output_rows)
    # Ensure correct column order
    final_df = final_df[['time', 'open', 'high', 'low', 'close', 'volume']]
    
    final_df.to_csv(FILE_NAME, index=False)
    print(f"SUCCESS: Generated {len(final_df)} days of data (2020-2026).")

if __name__ == "__main__":
    run_full_cycle()
