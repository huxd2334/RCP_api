from turtledemo.penrose import start

import pandas as pd
import requests
from datetime import datetime
from statistics import mean
import logging

from sympy.strategies.core import switch

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
VC_API_KEY = "YZ9TAWBYRZ5LM6777JR46FX4N"

def api_call(location, start_date, end_date):
    key=VC_API_KEY
    response = requests.request("GET", "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{}/{}/{}?unitGroup=metric&include=days&key={}&contentType=json".format(location, start_date, end_date, key ))
    if response.status_code != 200:
      print('Unexpected Status code: ', response.status_code)
      return None
    # Parse the results as JSON
    jsonData = response.json()
    return jsonData['days']

# api_call('10.510542,105.248554', '2022-03-18', '2022-03-18')

def get_start_date(season, harvest_date):
    try:
        date_obj = datetime.strptime(harvest_date, '%d-%m-%Y')
        today = datetime.today()
        # If harvest date is in the future, use today's date
        if date_obj > today:
            date_obj = today

        year = date_obj.year
        month = date_obj.month

        if season == "SA":
            # Summer Agricultural season always starts on April 1st
            return f"{year}-04-01"

        elif season == "WS":
            # Winter Season logic
            if month >= 11:
                return f"{year}-11-01"
            elif month <= 5:
                return f"{year - 1}-11-01"
            else:
                return None

    except Exception as e:
        logger.error(f"Error in get_start_date: {e}", exc_info=True)
        return None
# print(get_start_date("WS", "23-11-2024"))

def get_weather_data(longitude, latitude, season, date):
    try:
        features = ['humidity', 'precip','datetime' ]
        start_date = get_start_date(season, date)
        if start_date is None:
            logger.error('Invalid start date for weather data')
            return None
        end_date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
        today = datetime.today().strftime('%Y-%m-%d')
        if end_date > today:
            end_date = today
        location = f"{latitude},{longitude}"
        data = api_call(location, start_date, end_date)
        if data is None:
            logger.error('Failed to fetch data from API')
            return None
        df = pd.DataFrame(data)
        df = df[features]
        df['datetime'] = pd.to_datetime(df['datetime'])
        humidity = mean(df['humidity'])
        precip = mean(df['precip'])
        logger.info(f"Humidity: {humidity}")
        logger.info(f"Precipitation: {precip}")
        return  humidity, precip
    except Exception as e:
        logger.error(f"Error in get_weather_data: {e}", exc_info=True)
        return None


# x = get_weather_data(105.248554, 10.510542, 'WS', "23-11-2024")
# print(x)

# print(start, end)