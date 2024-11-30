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

PC_API_KEY = "st=2024-10-30T04%3A39%3A47Z&se=2024-10-31T05%3A24%3A47Z&sp=rl&sv=2024-05-04&sr=c&skoid=9c8ff44a-6a2c-4dfb-b298-1c9212f64d9a&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2024-10-31T01%3A20%3A26Z&ske=2024-11-07T01%3A20%3A26Z&sks=b&skv=2024-05-04&sig=yxW50NrgqfNEwh6gA5GpPmjbepQ0PP8d0LIG7B5GgAU%3D"
pc.settings.set_subscription_key(PC_API_KEY)
# Others
from datetime import datetime
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

def process_landsat_data(harvest_date, lon, lat, box_deg=0.10, L_savi=0.5, C1=6, C2=7.5, L_evi=1):
    # start_time = time.time()  # Start timing
    # Set a time window for 8 days before & after the date of harvest
    harvest_date = pd.to_datetime(harvest_date, dayfirst=True)

    day_offset = pd.Timedelta(days=8)
    today = pd.Timestamp.today()
    if harvest_date > today:
        harvest_date = today
        start_date = harvest_date - day_offset*2
        end_date = harvest_date
        time_range = f"{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
    else:
        start_date = harvest_date - day_offset
        end_date = harvest_date + day_offset
        time_range = f"{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"

    bbox = [lon - box_deg / 2, lat - box_deg / 2, lon + box_deg / 2, lat + box_deg / 2]
    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")
    # Search and load data from Landsat
    search = catalog.search(
        collections=["landsat-c2-l2"],
        bbox=bbox,
        datetime=time_range,
        query={'platform': {"in": ["landsat-8", "landsat-9"]}}
    )
    items = list(search.get_all_items())
    if not items:
        return None

    # Load bands of interest and mask
    xx = stac_load(
        items,
        bands=['red', 'nir08', 'qa_pixel', 'green', 'blue', 'swir16'],
        crs='EPSG:4326',
        resolution=30 / 111320,
        patch_url=pc.sign,
        bbox=bbox
    )

    # Apply scaling and offset
    xx['red'] = xx['red'] * 0.0000275 - 0.2
    xx['nir08'] = xx['nir08'] * 0.0000275 - 0.2
    xx['green'] = xx['green'] * 0.0000275 - 0.2
    xx['blue'] = xx['blue'] * 0.0000275 - 0.2
    xx['swir16'] = xx['swir16'] * 0.0000275 - 0.2

    # Create mask
    quality_mask = get_flags_to_mask(xx['qa_pixel'], ['fill', 'dilated_cloud', 'cirrus', 'cloud', 'shadow', 'water'])

    masked_data = xx.where(~quality_mask)
    clean_data = masked_data.mean(dim=['longitude', 'latitude']).compute()

    # Calculate indices
    ndvi = (clean_data.nir08 - clean_data.red) / (clean_data.nir08 + clean_data.red)
    savi = ((clean_data.nir08 - clean_data.red) / (clean_data.nir08 + clean_data.red + L_savi)) * (1 + L_savi)
    evi = 2.5 * ((clean_data.nir08 - clean_data.red) / (clean_data.nir08 + C1 * clean_data.red - C2 * clean_data.blue + L_evi))
    ndwi = (clean_data.green - clean_data.swir16) / (clean_data.green + clean_data.swir16)
    avi = np.power((clean_data.nir08 * (1 - clean_data.red) * (clean_data.nir08 - clean_data.red)), 1 / 3)
    ndmi = (clean_data.nir08 - clean_data.swir16) / (clean_data.nir08 + clean_data.swir16)
    albedo = 0.356 * clean_data.blue + 0.130 * clean_data.green + 0.373 * clean_data.red + 0.085 * clean_data.nir08 + 0.072 * clean_data.swir16 + 0.0018

    # Extract values
    # savi_vals = savi.values
    # evi_vals = evi.values
    ndwi_vals = ndwi.values
    # avi_vals = avi.values
    ndmi_vals = ndmi.values
    albedo_vals = albedo.values
    ndvi_vals = ndvi.values

# Calculate and return means
    means = {
        # 'mean_savi': np.mean(savi_vals),
        # 'mean_evi': np.mean(evi_vals),
        'mean_ndwi': np.mean(ndwi_vals),
        # 'mean_avi': np.mean(avi_vals),
        'mean_ndmi': np.mean(ndmi_vals),
        'mean_albedo': np.mean(albedo_vals),
        'mean_ndvi': np.mean(ndvi_vals)
    }
    # end_time = time.time()  # End timing
    # elapsed_time = end_time - start_time
    # logger.info(f"Function process_landsat_data took {elapsed_time:.2f} seconds to execute")
    logger.info(f"Means: {means}")
    return (
        # means['mean_savi'],
        # means['mean_evi'],
        means['mean_ndvi'],
        means['mean_ndwi'],
        # means['mean_avi'],
        means['mean_ndmi'],
        means['mean_albedo']
    )



# Test the function
# savi, evi, ndvi, ndwi, avi, ndmi = process_landsat_data("30-12-2024",  105.248554,10.510542 )
# print(f"Savi: {savi}, EVI: {evi}, NDVI: {ndvi}, NDWI: {ndwi}, AVI: {avi}, NDMI: {ndmi}")


# date = find_window_size("WS", "23-11-2024")
# print(date)