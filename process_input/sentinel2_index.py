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
PC_API_KEY ="st=2024-10-30T04%3A39%3A47Z&se=2024-10-31T05%3A24%3A47Z&sp=rl&sv=2024-05-04&sr=c&skoid=9c8ff44a-6a2c-4dfb-b298-1c9212f64d9a&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2024-10-31T01%3A20%3A26Z&ske=2024-11-07T01%3A20%3A26Z&sks=b&skv=2024-05-04&sig=yxW50NrgqfNEwh6gA5GpPmjbepQ0PP8d0LIG7B5GgAU%3D"
pc.settings.set_subscription_key(PC_API_KEY)


def fetch_and_process_data(lon, lat, date):
        box_size_deg = 0.0004  # ~ 5x5 px
        resolution = 10  # meters per px
        scale = resolution / 111320.0  # degrees per px

        date_object = datetime.strptime(date, "%d-%m-%Y")
        today = datetime.today()

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

        bbox = (lon - box_size_deg / 2, lat - box_size_deg / 2, lon + box_size_deg / 2, lat + box_size_deg / 2)


        # Search for Sentinel-2 data
        stac = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        search = stac.search(collections=["sentinel-2-l2a"], bbox=bbox, datetime=time_window)
        items = list(search.get_all_items())

        # Load the satellite data
        satellite_data = stac_load(
            items,
            bands=["red",  "nir",  "SCL"],
            crs="EPSG:4326",  # Latitude-Longitude
            resolution=scale,  # Degree resolution
            chunks={"x": 2048, "y": 2048},
            dtype="uint16",
            patch_url=pc.sign,
            bbox=bbox
        )

        # Cloud filtering process
        cloud_mask = \
            (satellite_data.SCL != 0) & \
            (satellite_data.SCL != 1) & \
            (satellite_data.SCL != 3) & \
            (satellite_data.SCL != 6) & \
            (satellite_data.SCL != 8) & \
            (satellite_data.SCL != 9) & \
            (satellite_data.SCL != 10)

        # Apply cloud mask to remove cloudy pixels
        clean_data = satellite_data.where(cloud_mask).astype("uint16")
        clean = clean_data.mean(dim=['longitude', 'latitude']).compute()

        # Calculate LAI
        lai_values = 0.618 * ((clean.nir - clean.red) / (clean.nir + clean.red)) ** 1.334
        # print(f"LAI: {lai_clean}")
        # Handle potential NaN values
        lai_numeric = lai_values.values
        valid_lai = lai_numeric[~np.isnan(lai_numeric)]

        # Calculate mean of valid LAI values
        if len(valid_lai) > 0:
            lai_mean = float(np.mean(valid_lai))
            # print(f"LAI for {lon}, {lat} on {date}: {lai_mean}")
            logging.info(f"LAI for {lon}, {lat} on {date}: {lai_mean}")
            return lai_mean
        else:
            # print(f"No valid LAI values found for {lon}, {lat} on {date}")
            logging.warning(f"No valid LAI values found for {lon}, {lat} on {date}")
            return np.nan

        # return lai_clean

# lai = fetch_and_process_data(105.248554, 10.510542, "15-07-2022")
# print(lai)