import warnings

import pystac_client
from sklearn.preprocessing import MinMaxScaler

warnings.filterwarnings('ignore')

# Import common GIS tools
import numpy as np

# Import Planetary Computer tools
from pystac_client import Client
import planetary_computer as pc
from odc.stac import stac_load
import logging
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
PC_API_KEY = ""
pc.settings.set_subscription_key(PC_API_KEY)

# calculate rvi index
def extract_sen1_data(lon, lat, date):
    # A = 0.71
    # B = 1.40
    box_size_deg = 0.0004 # ~ 5x5 px
    resolution = 10 # meters per px
    scale = resolution / 111320.0 # degrees per px

  # Load the data using Open Data Cube for each location
    date_object = datetime.strptime(date, "%d-%m-%Y")
    today  = datetime.today()

    if date_object > today:
        date_object = today
        print(f"Date is in the future. Using today's date: {date_object}")
        time_delta = timedelta(days=12)
        start_date = date_object - time_delta
        end_date = date_object
        time_window = f"{start_date.isoformat()}/{end_date.isoformat()}"
    else:
        time_delta = timedelta(days=6)
        start_date = date_object - time_delta
        end_date = date_object + time_delta
        time_window = f"{start_date.isoformat()}/{end_date.isoformat()}"

    bbox = (lon - box_size_deg/2, lat - box_size_deg/2, lon + box_size_deg/2, lat + box_size_deg/2)

    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    search = catalog.search(collections=["sentinel-1-rtc"], bbox=bbox, datetime=time_window)
    items = list(search.get_all_items())
    data = stac_load(items, bands=["vv", "vh"], patch_url=pc.sign, bbox=bbox, crs="EPSG:4326", resolution=scale)

    mean_values = data.mean(dim=["longitude", "latitude"]).compute()
    rvi_values = (4 * mean_values.vh / (mean_values.vv + mean_values.vh))
    rvi_numeric = rvi_values.values
    valid_rvi = rvi_numeric[~np.isnan(rvi_numeric)]
    if len(valid_rvi) > 0:
        rvi_mean = float(np.mean(valid_rvi))
        logging.info(f"RVI for {lon}, {lat} on {date}: {rvi_mean}")
        return rvi_mean
    else:
        logging.warning(f"No valid RVI values found for {lon}, {lat} on {date}")
        return np.nan


    # Extract VV and VH arrays
    # vv_array = data.vv.compute().values
    # vh_array = data.vh.compute().values
    # vv_mean = np.nanmean(vv_array)

    # Calculate RVI array
    # rvi_array = (4 * vh_array / (vv_array + vh_array))
    # def scale_data(array):
    #     flat_array = array.flatten()
    #     flat_array = flat_array[~np.isnan(flat_array)]  # Remove NaN values
    #     flat_array = flat_array.reshape(-1, 1)  # Reshape for scaler
    #     scaler = MinMaxScaler(feature_range=(0, 1))
    #     try:
    #         scaled = scaler.fit_transform(flat_array)
    #         mean = np.nanmean(scaled)
    #     except Exception as e:
    #         logger.error(f"Error in scaling data: {e}")
    #         mean = np.nan
    #     return mean
    #
    # with ThreadPoolExecutor() as executor:
    #     rvi_future = executor.submit(scale_data, rvi_array)
    #     sm_future = executor.submit(scale_data, sm_array)
    #
    #     rvi_mean = rvi_future.result()
    #     sm_mean = sm_future.result()

    # logger.info(f"RVI: {rvi_mean}")
    # logger.info(f"VV: {vv_mean}")
    # logger.info(f"SM: {sm_mean}")
    # return rvi_mean, sm_mean, vv_mean

#
# rvi = extract_sen1_data(105.248554, 10.510542, "15-07-2022")
# print(rvi)






