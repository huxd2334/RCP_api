# Supress Warnings
import warnings
warnings.filterwarnings('ignore')
# Import common GIS tools
import numpy as np
import pandas as pd

# Import Planetary Computer tools
import pystac_client
import planetary_computer as pc
from odc.stac import stac_load
from pystac.extensions.eo import EOExtension as eo
PC_API_KEY ="st=2024-10-30T04%3A39%3A47Z&se=2024-10-31T05%3A24%3A47Z&sp=rl&sv=2024-05-04&sr=c&skoid=9c8ff44a-6a2c-4dfb-b298-1c9212f64d9a&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2024-10-31T01%3A20%3A26Z&ske=2024-11-07T01%3A20%3A26Z&sks=b&skv=2024-05-04&sig=yxW50NrgqfNEwh6gA5GpPmjbepQ0PP8d0LIG7B5GgAU%3D"
pc.settings.set_subscription_key(PC_API_KEY)
# Others
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import logging



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# use the bit values of the 16-bit qa_pixel flag to mask the pixels and find clouds or water
bit_flags = {
            'fill': 1<<0,
            'dilated_cloud': 1<<1,
            'cirrus': 1<<2,
            'cloud': 1<<3,
            'shadow': 1<<4,
            'snow': 1<<5,
            'clear': 1<<6,
            'water': 1<<7
}

# func: mask pixels with a give type:
def get_flags_to_mask(mask, flags):
  combine_flag_mask = np.zeros_like(mask, dtype=bool)
  for flag in flags:
    current_flag_mask = np.bitwise_and(mask, bit_flags[flag])>0
    combine_flag_mask = combine_flag_mask | current_flag_mask
  return combine_flag_mask

# find window_size
# def find_window_size(season, harvest_date):
#     logger = logging.getLogger(__name__)
#     try:
#         date_obj = datetime.strptime(harvest_date, '%d-%m-%Y')
#         today = datetime.today()
#
#         # If harvest date is in future, use today's date
#         if date_obj > today:
#             date_obj = today
#
#         year = date_obj.year
#         month = date_obj.month
#         doh = date_obj.strftime('%Y-%m-%d')  # date of harvest
#
#         if season == "WS":
#             # For WS, if current month is after October, use current year
#             # Otherwise, use previous year
#             if month >= 11:
#                 window_start = f"{year}-11-01"
#             else:
#                 window_start = f"{year - 1}-11-01"
#
#             window_size = f"{window_start}/{doh}"
#             logger.info(f"Window size for WS: {window_size}")
#
#         elif season == "SA":
#             window_size = f"{year}-04-01/{doh}"
#             logger.info(f"Window size for SA: {window_size}")
#
#         else:
#             raise ValueError(f"Invalid season: {season}")
#
        # return window_size
#
#     except Exception as e:
#         logger.error(f"Error in find_window_size: {e}", exc_info=True)
#         raise

# def get_ls_index(longitude, latitude, season, date, box_deg=0.10):
#     logger = logging.getLogger(__name__)
#
#     try:
#         # Check if date is in the future
#         date_obj = datetime.strptime(date, '%d-%m-%Y')
#         today = datetime.today()
#         if date_obj > today:
#             date = today.strftime('%d-%m-%Y')
#             logger.warning(f"Date is in the future. Using today's date: {date}")
#         logger.info(
#             f"Fetching LS index for Longitude: {longitude}, Latitude: {latitude}, Season: {season}, Date: {date}")
#         catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
#
#         time_range = find_window_size(season, date)
#         logger.info(f"Time range: {time_range}")
#
#         bbox = [
#             longitude - box_deg / 2,
#             latitude - box_deg / 2,
#             longitude + box_deg / 2,
#             latitude + box_deg / 2
#         ]
#         logger.info(f"BBox: {bbox}")
#
#         # Search for relevant satellite images from the catalog
#         search = catalog.search(
#             collections=["landsat-c2-l2"],
#             bbox=bbox,
#             datetime=time_range,
#             query={
#                 'eo:cloud_cover': {"lt": 10},  # 10% cloud cover
#                 'platform': {"in": ["landsat-8", "landsat-9"]}
#             })
#         items = search.get_all_items()
#         if not items:
#             logger.warning("No images found.")
#             return np.nan, np.nan, np.nan, np.nan
#         logger.info(f"Found {len(items)} images")
#
#         # Select the image with the least cloud cover
#         selected_item = min(items, key=lambda item: eo.ext(item).cloud_cover)
#         bands_interest = ['red', 'nir08', 'qa_pixel', 'green', 'blue', 'swir16']
#
#         xx = stac_load(
#             [selected_item],
#             bands=bands_interest,
#             crs='EPSG:4326',
#             resolution=30 / 111320,
#             patch_url=pc.sign,
#             bbox=bbox).isel(time=0)
#
#         # Apply scaling and offset for the bands
#         xx['red'] = xx['red'] * 0.0000275 - 0.2
#         xx['nir08'] = xx['nir08'] * 0.0000275 - 0.2
#         xx['green'] = xx['green'] * 0.0000275 - 0.2
#         xx['blue'] = xx['blue'] * 0.0000275 - 0.2
#         xx['swir16'] = xx['swir16'] * 0.0000275 - 0.2
#
#         # Mask invalid data
#         quality_mask = get_flags_to_mask(xx['qa_pixel'],
#                                          ['fill', 'dilated_cloud', 'cirrus', 'cloud', 'shadow', 'water'])
#         masked_data = xx.where(~quality_mask)
#         clean_data = masked_data.mean(dim=['longitude', 'latitude']).compute()
#
#         # Cleaned bands
#         nir08 = clean_data.nir08.item()
#         red = clean_data.red.item()
#         green = clean_data.green.item()
#         blue = clean_data.blue.item()
#         swir16 = clean_data.swir16.item()
#
#         # Calculate index
#         ndvi = (nir08 - red) / (nir08 + red)
#         ndwi = (green - swir16) / (green + swir16)
#         avi = np.power((nir08 * (1 - red) * (nir08 - red)), 1 / 3)
#         ndmi = (nir08 - swir16) / (nir08 + swir16)
#
#         logger.info(f"Indices calculated - NDVI: {ndvi}, NDWI: {ndwi}, AVI: {avi}, NDMI: {ndmi}")
#         return ndvi, ndwi, avi, ndmi
#
#     except Exception as e:
#         logger.error(f"Error in get_ls_index: {e}", exc_info=True)
#         raise

def process_landsat_data(lat, lon, time_slice):
    G = 2.5
    C1 = 6
    C2 = 7.5
    L = 1
    resolution = 30 # meters per pixel
    scale = resolution/111320.0 # degrees per pixel
    box_size_deg = 0.10 # bounding box in degrees

    min_lon = lon - box_size_deg / 2
    min_lat = lat - box_size_deg / 2
    max_lon = lon + box_size_deg / 2
    max_lat = lat + box_size_deg / 2

    time_of_interest = time_slice

    bbox_of_interest = (min_lon, min_lat, max_lon, max_lat)

    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    search = catalog.search(
      collections=["landsat-c2-l2"],
      bbox=bbox_of_interest,
      datetime=time_of_interest,
      query={'platform': {"in": ["landsat-8", "landsat-9"]},})
    items = list(search.get_all_items())

    xx = stac_load(
        items,
        bands=['red', 'nir08', 'qa_pixel', 'green', 'swir16', 'blue', 'swir22'],
        crs='EPSG:4326',
        resolution=scale,
        patch_url=pc.sign,
        chunks={"x": 2048, "y": 2048},
        bbox= bbox_of_interest, )

    xx['red'] = (xx['red'] * 0.0000275) - 0.2
    xx['green'] = (xx['green'] * 0.0000275) - 0.2
    xx['blue'] = (xx['blue'] * 0.0000275) - 0.2
    xx['nir08'] = (xx['nir08'] * 0.0000275) - 0.2
    xx['swir16'] = xx['swir16'] * 0.0000275 - 0.2

    quality_mask = get_flags_to_mask(xx['qa_pixel'], ['fill', 'dilated_cloud', 'cirrus', 'cloud', 'shadow', 'water'])
    masked_data = xx.where(~quality_mask)
    clean_data = masked_data.mean(dim=['longitude', 'latitude']).compute()

    red = clean_data.red.mean().item()
    green = clean_data.green.mean().item()
    blue = clean_data.blue.mean().item()
    nir08 = clean_data.nir08.mean().item()
    swir16 = clean_data.swir16.mean().item()

    ndvi = (nir08 - red) / (nir08 + red)
    savi = ((nir08 - red) / (nir08 + red + L)) * (1 + L)
    # evi = G * ((nir08 - red) / (nir08 + C1 * red - C2 * blue + L))
    ndwi = (green - swir16) / (green + swir16)
    # avi = np.power((nir08 * (1 - red) * (nir08 - red)), 1 / 3)
    ndmi = (nir08 - swir16) / (nir08 + swir16)
    # albedo = 0.356 * blue + 0.130 * green + 0.373 * red + 0.085 * nir08 + 0.072 * swir16 + 0.0018

    # logging.info(f"NDVI: {ndvi}, SAVI: {savi}, NDWI: {ndwi}, NDMI: {ndmi}")

    return ndvi, savi, ndwi, ndmi

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

# time_slice_ws1, time_slice_ws2, time_slice_ws3 = define_time_slice("23-06-2024", "SA")
# print(time_slice_ws1, time_slice_ws2, time_slice_ws3)

from concurrent.futures import ThreadPoolExecutor, as_completed

def get_indices_by_stage(lat, lon, doh, season):
    time_slice1, time_slice2, time_slice3 = define_time_slice(doh, season)
    logging.info(f"Time slices: {time_slice1}, {time_slice2}, {time_slice3}")

    def process_if_not_none(time_slice):
        return process_landsat_data(lat, lon, time_slice) if time_slice else (np.nan, np.nan, np.nan, np.nan)

    indices = [None, None, None]
    time_slices = [time_slice1, time_slice2, time_slice3]

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_if_not_none, ts): i for i, ts in enumerate(time_slices)}

        for future in as_completed(futures):
            index = futures[future]
            indices[index] = future.result()

    logging.info(
        f"NDVI_1: {indices[0][0]}, SAVI_1: {indices[0][1]}, NDWI_1: {indices[0][2]}, NDMI_1: {indices[0][3]}\n"
        f"NDVI_2: {indices[1][0]}, SAVI_2: {indices[1][1]}, NDWI_2: {indices[1][2]}, NDMI_2: {indices[1][3]}\n"
        f"NDVI_3: {indices[2][0]}, SAVI_3: {indices[2][1]}, NDWI_3: {indices[2][2]}, NDMI_3: {indices[2][3]}"
    )

    return indices[0], indices[1], indices[2]
# def get_indices_by_stage(lat, lon, doh, season):
#     time_slice1, time_slice2, time_slice3 = define_time_slice(doh, season)
#     logging.info(f"Time slices: {time_slice1}, {time_slice2}, {time_slice3}")
#
#     def process_if_not_none(time_slice):
#         return process_landsat_data(lat, lon, time_slice) if time_slice else (np.nan, np.nan, np.nan, np.nan)
#
#     with ThreadPoolExecutor() as executor:
#         future1 = executor.submit(process_if_not_none, time_slice1)
#         future2 = executor.submit(process_if_not_none, time_slice2)
#         future3 = executor.submit(process_if_not_none, time_slice3)
#
#         indices1 = future1.result()
#         indices2 = future2.result()
#         indices3 = future3.result()
#
#         logging.info(
#                      f"NDVI_1: {indices1[0]}, SAVI_1: {indices1[1]}, NDWI_1: {indices1[2]}, NDMI_1: {indices1[3]}\n"
#                      f"NDVI_2: {indices2[0]}, SAVI_2: {indices2[1]}, NDWI_2: {indices2[2]}, NDMI_2: {indices2[3]}\n"
#                      f"NDVI_3: {indices3[0]}, SAVI_3: {indices3[1]}, NDWI_3: {indices3[2]}, NDMI_3: {indices3[3]}",)
#
#     return indices1, indices2, indices3
#
#
#
#
#
#
#
#
#
#     # rs = get_indices_by_stage(10.510542, 105.248554, "03-07-2024", "SA")
#     # # Unpack the results
#     # ndvi_1, savi_1, ndwi_1, ndmi_1 = rs[0]
#     # ndvi_2, savi_2, ndwi_2, ndmi_2 = rs[1]
#     # ndvi_3, savi_3, ndwi_3, ndmi_3 = rs[2]
#     #
#     # # Print the values
#     # print(f"ndvi_1: {ndvi_1}, ndvi_2: {ndvi_2}, ndvi_3: {ndvi_3}")
