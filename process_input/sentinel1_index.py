import warnings

import pystac_client
from sklearn.preprocessing import MinMaxScaler

warnings.filterwarnings('ignore')

# Import common GIS tools
import numpy as np

import os
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
PC_API_KEY = "st=2024-10-30T04%3A39%3A47Z&se=2024-10-31T05%3A24%3A47Z&sp=rl&sv=2024-05-04&sr=c&skoid=9c8ff44a-6a2c-4dfb-b298-1c9212f64d9a&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2024-10-31T01%3A20%3A26Z&ske=2024-11-07T01%3A20%3A26Z&sks=b&skv=2024-05-04&sig=yxW50NrgqfNEwh6gA5GpPmjbepQ0PP8d0LIG7B5GgAU%3D"
pc.settings.set_subscription_key(PC_API_KEY)

# calculate rvi index
def extract_sen1_data(lon, lat, date):
    A = 0.71
    B = 1.40
    box_size_deg = 0.0004 # ~ 5x5 px
    resolution = 10 # meters per px
    scale = resolution / 111320.0 # degrees per px

  # Load the data using Open Data Cube for each location
    date_object = datetime.strptime(date, "%d-%m-%Y")
    today  = datetime.today()

    if date_object > today:
        date_object = today
        print(f"Date is in the future. Using today's date: {date_object}")
        time_delta = timedelta(days=11)
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

    # Extract VV and VH arrays
    vv_array = data.vv.compute().values
    vh_array = data.vh.compute().values
    vv_mean = np.nanmean(vv_array)

    # Calculate RVI array
    rvi_array = (4 * vh_array / (vv_array + vh_array))

    # Calculate Soil moisture index
    dop_array = vv_array / (vv_array + vh_array)
    sm_array = 1 - ((10 ** (0.1 * dop_array)) / A) ** B

    # rvi_flat = rvi_array.flatten()
    # rvi_flat = rvi_flat[~np.isnan(rvi_flat)]  # Remove NaN values
    # rvi_flat = rvi_flat.reshape(-1, 1)  # Reshape for scaler
    #
    # # Scale RVI between 0 and 1
    # rvi_scaler = MinMaxScaler(feature_range=(0, 1))
    # try:
    #     rvi_scaled = rvi_scaler.fit_transform(rvi_flat)
    #     rvi_mean = np.nanmean(rvi_scaled)
    # except Exception as e:
    #     logger.error(f"Error in scaling RVI: {e}")
    #     rvi_mean = np.nan
    #
    # sm_flat = sm_array.flatten()
    # sm_flat = sm_flat[~np.isnan(sm_flat)]  # Remove NaN values
    # sm_flat = sm_flat.reshape(-1, 1)  # Reshape for scaler
    #
    # # Scale RVI between 0 and 1
    # sm_scaler = MinMaxScaler(feature_range=(0, 1))
    # try:
    #     sm_scaled = sm_scaler.fit_transform(sm_flat)
    #     sm_mean = np.nanmean(sm_scaled)
    # except Exception as e:
    #     logger.error(f"Error in scaling SM: {e}")
    #     sm_mean = np.nan
    #
    # print(f"RVI: {rvi_mean}")
    # print(f"VV: {vv_mean}")
    # print(f"SM: {sm_mean}")
    def scale_data(array):
        flat_array = array.flatten()
        flat_array = flat_array[~np.isnan(flat_array)]  # Remove NaN values
        flat_array = flat_array.reshape(-1, 1)  # Reshape for scaler
        scaler = MinMaxScaler(feature_range=(0, 1))
        try:
            scaled = scaler.fit_transform(flat_array)
            mean = np.nanmean(scaled)
        except Exception as e:
            logger.error(f"Error in scaling data: {e}")
            mean = np.nan
        return mean

    with ThreadPoolExecutor() as executor:
        rvi_future = executor.submit(scale_data, rvi_array)
        sm_future = executor.submit(scale_data, sm_array)

        rvi_mean = rvi_future.result()
        sm_mean = sm_future.result()

    logger.info(f"RVI: {rvi_mean}")
    logger.info(f"VV: {vv_mean}")
    logger.info(f"SM: {sm_mean}")
    return rvi_mean, sm_mean, vv_mean


# extract_sen1_data(105.192464	, 10.467721, "15-07-2022")






