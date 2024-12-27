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


import logging
from datetime import datetime

import logging
from datetime import datetime

import logging
from datetime import datetime

import logging
from datetime import datetime

import logging
from datetime import datetime

def define_time_slice(doh):
    date_obj = datetime.strptime(doh, '%d-%m-%Y')
    today = datetime.today()

    if date_obj > today:
        doh = today.strftime('%Y-%m-%d')
        logging.warning(f"Date is in the future. Using today's date: {doh}")
        date_obj = today
    else:
        doh = date_obj.strftime('%Y-%m-%d')

    year = date_obj.year

    def ws_time_slices():
        if date_obj > datetime(year, 4, 30):
            return f"{year}-01-01/{year}-02-28", f"{year}-03-01/{year}-04-30"
        else:
            return f"{year-1}-01-01/{year-1}-02-28", f"{year-1}-03-01/{year-1}-04-30"

    def sa_time_slices():
        if date_obj > datetime(year, 8, 31):
            return f"{year}-05-01/{year}-06-30", f"{year}-07-01/{year}-08-31"
        else:
            return f"{year-1}-05-01/{year-1}-06-30", f"{year-1}-07-01/{year-1}-08-31"

    ws_slice1, ws_slice2 = ws_time_slices()
    sa_slice1, sa_slice2 = sa_time_slices()

    return ws_slice1, ws_slice2, sa_slice1, sa_slice2

# Example usage
# ws1, ws2, sa1, sa2 = define_time_slice("15-01-2023")
# print(ws1, ws2, sa1, sa2)



def process_vvvh(lat, lon, time_slice):
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

    return vh, vv

def get_vvvh_by_stage(lat, lon, doh):
    ws1, ws2, sa1, sa2 = define_time_slice(doh)
    logging.info(f"WS1: {ws1}, WS2: {ws2}, SA1: {sa1}, SA2: {sa2}")

    def process_if_not_none(time_slice):
        return process_vvvh(lat, lon, time_slice) if time_slice else (np.nan)

    with ThreadPoolExecutor() as executor:
        future1 = executor.submit(process_if_not_none, ws1)
        future2 = executor.submit(process_if_not_none, ws2)
        future3 = executor.submit(process_if_not_none, sa1)
        future4 = executor.submit(process_if_not_none, sa2)

        indices1 = future1.result()
        indices2 = future2.result()
        indices3 = future3.result()
        indices4 = future4.result()

        logging.info(f"WS1: {indices1}, WS2: {indices2}, SA1: {indices3}, SA2: {indices4}")

    return indices4, indices2, indices3, indices1

# id1, id2, id3, id4 = get_vvvh_by_stage(10.510542, 105.248554, "15-07-2022")
# print(f"WS1: {id1}, WS2: {id2}, SA1: {id3}, SA2: {id4}")
# print(f"RVI_1: {rvi_1}, RVI_2: {rvi_2}, RVI_3: {rvi_3}")





