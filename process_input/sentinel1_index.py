# Supress Warnings
import warnings
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
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        today = datetime.today()

        # If harvest date is in future, use today's date
        if date_obj > today:
            date_obj = today

        year = date_obj.year
        month = date_obj.month

        if season == "SA":
            plant_date = f"01-04-{year}"
            logger.info(f"Plant Date for SA: {plant_date}")
        elif season == "WS":
            # For WS, if current month is after October, use current year
            # Otherwise, use previous year
            if month >= 11:
                plant_date = f"01-11-{year}"
            else:
                plant_date = f"01-11-{year - 1}"
            logger.info(f"Plant Date for WS: {plant_date}")
        else:
            raise ValueError(f"Invalid season: {season}")

        return plant_date

    except Exception as e:
        logger.error(f"Error in find_plant_date: {e}", exc_info=True)
        raise


def process_single_date(date, longitude, latitude):
    try:
        box_deg = 0.0004
        window_start = date - timedelta(days=6)
        window_end = date + timedelta(days=6)

        catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
        search = catalog.search(
            collections=["sentinel-1-rtc"],
            bbox=[longitude - box_deg / 2, latitude - box_deg / 2,
                  longitude + box_deg / 2, latitude + box_deg / 2],
            datetime=f"{window_start.strftime('%Y-%m-%d')}/{window_end.strftime('%Y-%m-%d')}")

        items = list(search.get_all_items())
        vv_vals, vh_vals = [], []

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

            dop = mean_vv / (mean_vv + mean_vh)
            rvi = (np.sqrt(dop)) * ((4 * mean_vh) / (mean_vv + mean_vh))
            return rvi
        return None

    except Exception as e:
        logging.error(f"Error processing date {date}: {e}")
        return None


def get_rvi_parallel(longitude, latitude, season, harvest_date, interval_days=12):
    plant_date = find_plant_date(season, harvest_date)
    plant_date = pd.to_datetime(plant_date, dayfirst=True)
    harvest_date = pd.to_datetime(harvest_date, dayfirst=True)

    # Generate dates
    dates = []
    current_date = plant_date
    while current_date <= harvest_date:
        dates.append(current_date)
        current_date += timedelta(days=interval_days)

    # Parallel processing
    rvi_values = []
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_date = {
            executor.submit(process_single_date, date, longitude, latitude): date
            for date in dates
        }

        for future in as_completed(future_to_date):
            rvi = future.result()
            if rvi is not None:
                rvi_values.append(rvi)

    # Calculate RVI features
    rvi_values = np.array(rvi_values)
    return (
        np.mean(rvi_values) if len(rvi_values) > 0 else np.nan,
        np.std(rvi_values) if len(rvi_values) > 0 else np.nan,
        np.max(rvi_values) if len(rvi_values) > 0 else np.nan,
        np.min(rvi_values) if len(rvi_values) > 0 else np.nan,
        np.max(rvi_values) - np.min(rvi_values) if len(rvi_values) > 0 else np.nan
    )


# Test
# rvi, std_rvi, max_rvi, min_rvi, range_rvi = get_rvi_parallel(105.248554, 10.510542, "WS", "28-04-2023")
# print(f"RVI: {rvi}, Std: {std_rvi}, Max: {max_rvi}, Min: {min_rvi}, Range: {range_rvi}")