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
PC_API_KEY = "st=2024-10-30T04%3A39%3A47Z&se=2024-10-31T05%3A24%3A47Z&sp=rl&sv=2024-05-04&sr=c&skoid=9c8ff44a-6a2c-4dfb-b298-1c9212f64d9a&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2024-10-31T01%3A20%3A26Z&ske=2024-11-07T01%3A20%3A26Z&sks=b&skv=2024-05-04&sig=yxW50NrgqfNEwh6gA5GpPmjbepQ0PP8d0LIG7B5GgAU%3D"
pc.settings.set_subscription_key(PC_API_KEY)

# calculate rvi index
def extract_sen1_data(lon, lat, date):
    try:
        box_size_deg = 0.0004  # ~ 5x5 px
        resolution = 10  # meters per px
        scale = resolution / 111320.0  # degrees per px

        date_object = datetime.strptime(date, "%d-%m-%Y")
        today = datetime.today()

        if date_object > today:
            date_object = today
            logger.info(f"Date is in the future. Using today's date: {date_object}")
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

        # Check if any items were found
        if not items:
            logger.warning(f"No Sentinel-1 data found for location ({lon}, {lat}) during time window {time_window}")
            return np.nan

        data = stac_load(items, bands=["vv", "vh"], patch_url=pc.sign, bbox=bbox, crs="EPSG:4326", resolution=scale)

        mean_values = data.mean(dim=["longitude", "latitude"]).compute()
        rvi_values = (4 * mean_values.vh / (mean_values.vv + mean_values.vh))
        rvi_numeric = rvi_values.values
        valid_rvi = rvi_numeric[~np.isnan(rvi_numeric)]
        
        if len(valid_rvi) > 0:
            rvi_mean = float(np.mean(valid_rvi))
            logger.info(f"RVI for {lon}, {lat} on {date}: {rvi_mean}")
            return rvi_mean
        else:
            logger.warning(f"No valid RVI values found for {lon}, {lat} on {date}")
            return np.nan
            
    except Exception as e:
        logger.error(f"Error in extract_sen1_data: {e}")
        return np.nan
#
# rvi = extract_sen1_data(105.248554, 10.510542, "15-07-2022")
# print(rvi)






