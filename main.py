import requests
import pandas as pd
import sqlite3
import time
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

# Constants
API_KEY = '14849c605245b24ac8ae30a0b848063fY'  # Replace with your OpenWeatherMap API key
CITIES = ['Delhi', 'Mumbai', 'Chennai', 'Bangalore', 'Kolkata', 'Hyderabad']
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
DB_NAME = 'weather_data.db'

# Initialize SQLite database
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_weather (
        date TEXT PRIMARY KEY,
        avg_temp REAL,
        max_temp REAL,
        min_temp REAL,
        dominant_condition TEXT
    )
''')
conn.commit()

# Function to fetch weather data
def fetch_weather_data(city):
    params = {
        'q': city,
        'appid': API_KEY,
        'units': 'metric'  # Get temperature in Celsius
    }
    response = requests.get(BASE_URL, params=params)
    return response.json()

# Function to process weather data
def process_weather_data():
    weather_records = []
    for city in CITIES:
        data = fetch_weather_data(city)
        if data['cod'] == 200:  # Check if the request was successful
            main = data['main']
            weather = data['weather'][0]
            weather_records.append({
                'city': city,
                'temp': main['temp'],
                'feels_like': main['feels_like'],
                'condition': weather['main'],
                'timestamp': data['dt']
            })
    
    # Calculate daily aggregates
    summarize_weather(weather_records)

def summarize_weather(records):
    df = pd.DataFrame(records)
    today = datetime.now().date().isoformat()
    
    # Calculate aggregates
    avg_temp = df['temp'].mean()
    max_temp = df['temp'].max()
    min_temp = df['temp'].min()
    dominant_condition = df['condition'].mode()[0]  # Most frequent condition

    # Store in database
    cursor.execute('''
        INSERT OR REPLACE INTO daily_weather (date, avg_temp, max_temp, min_temp, dominant_condition)
        VALUES (?, ?, ?, ?, ?)
    ''', (today, avg_temp, max_temp, min_temp, dominant_condition))
    conn.commit()

# Scheduler for periodic data fetching
scheduler = BackgroundScheduler()
scheduler.add_job(process_weather_data, 'interval', minutes=5)
scheduler.start()

# Run the scheduler
try:
    while True:
        time.sleep(1)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
    conn.close()