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
# PC_API_KEY ="st=2024-10-30T04%3A39%3A47Z&se=2024-10-31T05%3A24%3A47Z&sp=rl&sv=2024-05-04&sr=c&skoid=9c8ff44a-6a2c-4dfb-b298-1c9212f64d9a&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2024-10-31T01%3A20%3A26Z&ske=2024-11-07T01%3A20%3A26Z&sks=b&skv=2024-05-04&sig=yxW50NrgqfNEwh6gA5GpPmjbepQ0PP8d0LIG7B5GgAU%3D"
# pc.settings.set_subscription_key(PC_API_KEY)
from concurrent.futures import ProcessPoolExecutor


# calculate rvi index
# def extract_sen1_data(lon, lat, date):
#     # A = 0.71
#     # B = 1.40
#     box_size_deg = 0.0004 # ~ 5x5 px
#     resolution = 10 # meters per px
#     scale = resolution / 111320.0 # degrees per px
#
#   # Load the data using Open Data Cube for each location
#     date_object = datetime.strptime(date, "%d-%m-%Y")
#     today  = datetime.today()
#
#     if date_object > today:
#         date_object = today
#         print(f"Date is in the future. Using today's date: {date_object}")
#         time_delta = timedelta(days=12)
#         start_date = date_object - time_delta
#         end_date = date_object
#         time_window = f"{start_date.isoformat()}/{end_date.isoformat()}"
#     else:
#         time_delta = timedelta(days=6)
#         start_date = date_object - time_delta
#         end_date = date_object + time_delta
#         time_window = f"{start_date.isoformat()}/{end_date.isoformat()}"
#
#     bbox = (lon - box_size_deg/2, lat - box_size_deg/2, lon + box_size_deg/2, lat + box_size_deg/2)
#
#     catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
#     search = catalog.search(collections=["sentinel-1-rtc"], bbox=bbox, datetime=time_window)
#     items = list(search.get_all_items())
#     data = stac_load(items, bands=["vv", "vh"], patch_url=pc.sign, bbox=bbox, crs="EPSG:4326", resolution=scale)
#
#     mean_values = data.mean(dim=["longitude", "latitude"]).compute()
#     rvi_values = (4 * mean_values.vh / (mean_values.vv + mean_values.vh))
#     rvi_numeric = rvi_values.values
#     valid_rvi = rvi_numeric[~np.isnan(rvi_numeric)]
#     if len(valid_rvi) > 0:
#         rvi_mean = float(np.mean(valid_rvi))
#         logging.info(f"RVI for {lon}, {lat} on {date}: {rvi_mean}")
#         return rvi_mean
#     else:
#         logging.warning(f"No valid RVI values found for {lon}, {lat} on {date}")
#         return np.nan
#
#
#     # Extract VV and VH arrays
#     # vv_array = data.vv.compute().values
#     # vh_array = data.vh.compute().values
#     # vv_mean = np.nanmean(vv_array)
#
#     # Calculate RVI array
#     # rvi_array = (4 * vh_array / (vv_array + vh_array))
#     # def scale_data(array):
#     #     flat_array = array.flatten()
#     #     flat_array = flat_array[~np.isnan(flat_array)]  # Remove NaN values
#     #     flat_array = flat_array.reshape(-1, 1)  # Reshape for scaler
#     #     scaler = MinMaxScaler(feature_range=(0, 1))
#     #     try:
#     #         scaled = scaler.fit_transform(flat_array)
#     #         mean = np.nanmean(scaled)
#     #     except Exception as e:
#     #         logger.error(f"Error in scaling data: {e}")
#     #         mean = np.nan
#     #     return mean
#     #
#     # with ThreadPoolExecutor() as executor:
#     #     rvi_future = executor.submit(scale_data, rvi_array)
#     #     sm_future = executor.submit(scale_data, sm_array)
#     #
#     #     rvi_mean = rvi_future.result()
#     #     sm_mean = sm_future.result()
#
#     # logger.info(f"RVI: {rvi_mean}")
#     # logger.info(f"VV: {vv_mean}")
#     # logger.info(f"SM: {sm_mean}")
#     # return rvi_mean, sm_mean, vv_mean

#
# rvi = extract_sen1_data(105.248554, 10.510542, "15-07-2022")
# print(rvi)

def define_time_slice(doh, season):
    date_obj = datetime.strptime(doh, '%d-%m-%Y')
    today = datetime.today()


    if date_obj > today:
        doh = today.strftime('%Y-%m-%d')
        logging.warning(f"Date is in the future. Using today's date: {doh}")
        date_obj = today
    else:
        doh = date_obj.strftime('%Y-%m-%d')

    year = date_obj.year
    month = date_obj.month

    def ws_time_slices():
        # we have 3 time slices for WS
        # 1. 11/01 - 12/31
        # 2. 01/01 - 02/28
        # 3. 03/01 - doh
        if month == 11 or month == 12:
            return f"{year}-11-01/{doh}", None, None
        elif month == 1 or month == 2:
            return f"{year-1}-11-01/{year-1}-12-31", f"{year}-01-01/{doh}", None
        elif month == 3:
            return f"{year-1}-11-01/{year-1}-12-31", f"{year}-01-01/{year}-02-28", f"{year}-03-01/{doh}"
        else:
            return None, None, None

    def sa_time_slices():
        # we have 3 time slices for SA
        # 1. 04/01 - 05/31
        # 2. 06/01 - 06/30
        # 3. 07/01 - doh
        if month == 4 or month == 5:
            return f"{year}-04-01/{doh}", None, None
        elif month == 6:
            return f"{year}-04-01/{year}-05-31", f"{year}-06-01/{doh}", None
        elif month >= 7:
            return f"{year}-04-01/{year}-05-31", f"{year}-06-01/{year}-06-30", f"{year}-07-01/{doh}"
        else:
            return None, None, None

    switch = {
        "WS": ws_time_slices,
        "SA": sa_time_slices
    }

    if season in switch:
        return switch[season]()
    else:
        raise ValueError(f"Invalid season: {season}")

def process_data(lat, lon, time_slice):
    box_size_deg = 0.0004  # ~ 5x5 px
    resolution = 10  # meters per px
    scale = resolution / 111320.0  # degrees per px

    min_lon = lon - box_size_deg / 2
    min_lat = lat - box_size_deg / 2
    max_lon = lon + box_size_deg / 2
    max_lat = lat + box_size_deg / 2

    bbox_of_interest = (min_lon, min_lat, max_lon, max_lat)

    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    search = catalog.search(collections=["sentinel-1-rtc"],
                            bbox=bbox_of_interest,
                            datetime=time_slice)
    items = list(search.get_all_items())
    data = stac_load(items,
                     bands=["vv", "vh"],
                     patch_url=pc.sign,
                     bbox=bbox_of_interest,
                     crs="EPSG:4326",
                     resolution=scale)

    vv = data['vv'].mean().item()
    vh = data['vh'].mean().item()
    dop = vv / (vv + vh)
    rvi = np.sqrt(dop) * (4 * vh / (vv + vh))
    return rvi

def get_rvi_by_stage(lat, lon, doh, season):
    time_slice1, time_slice2, time_slice3 = define_time_slice(doh, season)
    logging.info(f"Time slices: {time_slice1}, {time_slice2}, {time_slice3}")

    def process_if_not_none(time_slice):
        return process_data(lat, lon, time_slice) if time_slice else (np.nan)

    with ThreadPoolExecutor() as executor:
        future1 = executor.submit(process_if_not_none, time_slice1)
        future2 = executor.submit(process_if_not_none, time_slice2)
        future3 = executor.submit(process_if_not_none, time_slice3)

        indices1 = future1.result()
        indices2 = future2.result()
        indices3 = future3.result()

        logging.info(f"RVI_1: {indices1}, RVI_2: {indices2}, RVI_3: {indices3}")

    return indices1, indices2, indices3

# rvi_1, rvi_2, rvi_3 = get_rvi_by_stage(10.510542, 105.248554, "15-07-2022", "SA")
# print(f"RVI_1: {rvi_1}, RVI_2: {rvi_2}, RVI_3: {rvi_3}")





