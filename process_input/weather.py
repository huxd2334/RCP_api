import pandas as pd
import requests
from datetime import datetime
from statistics import mean
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def api_call(location, start_date, end_date):
    print('Fetching data for location: {} from {} to {}'.format(location, start_date, end_date))
    key='3NDWEP3GJVMRCSVRRZS6BT59K'
    response = requests.request("GET", "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{}/{}/{}?unitGroup=metric&include=days&key={}&contentType=json".format(location, start_date, end_date, key ))
    if response.status_code != 200:
      print('Unexpected Status code: ', response.status_code)
      return None
    # Parse the results as JSON
    jsonData = response.json()
    # print(jsonData)
    return jsonData['days']

# api_call('10.510542,105.248554', '2022-03-18', '2022-03-18')

def get_start_date(season, harvest_date):
    try:
        date_obj = datetime.strptime(harvest_date, '%d-%m-%Y')
        year = date_obj.year
        month = date_obj.month
        if season == "SA":
            start_date = f"{year}-04-01"
        elif (season == "WS" and month < 11):
            return None
        else:
            start_date = f"{year}-11-01"
        return start_date
    except Exception as e:
        logger.error(f"Error in get_start_date at weather.py: {e}", exc_info=True)
        return None
# print(get_start_date("WS", "18-11-2022"))

def get_weather_data(longitude, latitude, season, date):
    # features = ['tempmax', 'tempmin', 'temp', 'dew', 'humidity', 'precip',
    #             'precipcover', 'windgust', 'windspeed', 'pressure', 'cloudcover',
    #             'solarradiation', 'solarenergy', 'uvindex', 'sunrise', 'sunset']
    try:
        features = ['datetime', 'precip']
        start_date = get_start_date(season, date)
        if start_date is None:
            logger.error('Invalid start date')
            return None
        end_date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
        location = f"{latitude},{longitude}"
        data = api_call(location, start_date, end_date)
        if data is None:
            logger.error('Failed to fetch data from API')
            return None
        df = pd.DataFrame(data)
        df = df[features]
        df['datetime'] = pd.to_datetime(df['datetime'])
        precip = mean(df['precip'])
        return precip
    except Exception as e:
        logger.error(f"Error in get_weather_data: {e}", exc_info=True)
        return None


# get_weather_data(105.248554, 10.510542, 'WS', "2023-04-15")
# print(start, end)