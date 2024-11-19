# Supress Warnings
import warnings
warnings.filterwarnings('ignore')

# Import common GIS tools
import numpy as np
# Import Planetary Computer tools
from pystac_client import Client
import planetary_computer as pc
from odc.stac import stac_load
import logging
import pandas as pd
from datetime import timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
PC_API_KEY = ""
pc.settings.set_subscription_key(PC_API_KEY)

from datetime import datetime

def find_plant_date(season, harvest_date):
    logger = logging.getLogger(__name__)
    try:
        date_obj = datetime.strptime(harvest_date, '%d-%m-%Y')
        year = date_obj.year
        if season == "SA":
            plant_date = f"01-04-{year}"
            logger.info(f"Plant Date for SA: {plant_date}")
        elif season == "WS":
            plant_date = f"01-11-{year-1}"
            logger.info(f"Plant Date for WS: {plant_date}")
        else:
            raise ValueError(f"Invalid season sen: {season}")
        return plant_date

    except Exception as e:
        logger.error(f"Error in find_plant_date sen: {e}", exc_info=True)
        raise


def get_rvi(longitude, latitude, season, harvest_date, interval_days=12):
    # find plant date
    plant_date = find_plant_date(season, harvest_date)

    plant_date = pd.to_datetime(plant_date, dayfirst=True)
    # print(f"Plant Date: {plant_date}")
    harvest_date = pd.to_datetime(harvest_date, dayfirst=True)
    # print(f"Harvest Date: {harvest_date}\n")

    # Generate dates for RVI calculation
    dates = []
    current_date = plant_date
    while current_date <= harvest_date:
        dates.append(current_date)
        current_date += timedelta(days=interval_days)

    rvi_values = []

    for date in dates:
        # Calculate box size based on field size (adjust as needed)
        box_deg = 0.0004  # Approximately 40m x 40m

        # Get Sentinel-1 data for a shorter time window around each date
        window_start = date - timedelta(days=6)
        window_end = date + timedelta(days=6)

        vv_vals, vh_vals = [], []

        # Load data
        catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        search = catalog.search(
            collections=["sentinel-1-rtc"],
            bbox=[longitude - box_deg / 2, latitude - box_deg / 2,
                  longitude + box_deg / 2, latitude + box_deg / 2],
            datetime=f"{window_start.strftime('%Y-%m-%d')}/{window_end.strftime('%Y-%m-%d')}")

        items = list(search.get_all_items())

        for item in items:
            dt = stac_load(
                [item],
                bands=["vv", "vh"],
                patch_url=pc.sign,
                bbox=[longitude - box_deg / 2, latitude - box_deg / 2,
                      longitude + box_deg / 2, latitude + box_deg / 2],
                crs="EPSG:4326",
                resolution=10 / 111320.0)

            if np.all(dt["vv"].values != -32768.0) and np.all(dt["vh"].values != -32768.0):
                vv_vals.append(np.mean(dt["vv"].astype("float64")))
                vh_vals.append(np.mean(dt["vh"].astype("float64")))

        if vv_vals and vh_vals:
            mean_vv = np.mean(vv_vals)
            mean_vh = np.mean(vh_vals)

            # Calculate RVI
            dop = mean_vv / (mean_vv + mean_vh)
            rvi = (np.sqrt(dop)) * ((4 * mean_vh) / (mean_vv + mean_vh))

            rvi_values.append(rvi)

    # Calculate RVI features
    rvi_values = np.array(rvi_values)
    mean_rvi = np.mean(rvi_values) if len(rvi_values) > 0 else np.nan
    # std_rvi = np.std(rvi_values) if len(rvi_values) > 0 else np.nan
    # max_rvi = np.max(rvi_values) if len(rvi_values) > 0 else np.nan
    # min_rvi = np.min(rvi_values) if len(rvi_values) > 0 else np.nan
    # range_rvi = max_rvi - min_rvi if len(rvi_values) > 0 else np.nan


    return mean_rvi
# Test
# rvi = get_rvi(105.248554, 10.510542, "SA", "15-07-2023")
# print(rvi)