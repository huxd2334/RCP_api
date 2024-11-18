# Supress Warnings
import warnings
warnings.filterwarnings('ignore')

# Import common GIS tools
import numpy as np
# Import Planetary Computer tools
import pystac_client
import planetary_computer as pc
from odc.stac import stac_load
from pystac.extensions.eo import EOExtension as eo

pc.settings.set_subscription_key("st=2024-10-30T04%3A39%3A47Z&se=2024-10-31T05%3A24%3A47Z&sp=rl&sv=2024-05-04&sr=c&skoid=9c8ff44a-6a2c-4dfb-b298-1c9212f64d9a&sktid=72f988bf-86f1-41af-91ab-2d7cd011db47&skt=2024-10-31T01%3A20%3A26Z&ske=2024-11-07T01%3A20%3A26Z&sks=b&skv=2024-05-04&sig=yxW50NrgqfNEwh6gA5GpPmjbepQ0PP8d0LIG7B5GgAU%3D")

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
   # Initialize a mask to store results with the same shape as the input mask
  combine_flag_mask = np.zeros_like(mask, dtype=bool)
  for flag in flags:
    # Compute the mask for the current flag
    current_flag_mask = np.bitwise_and(mask, bit_flags[flag])>0
    # Combine the current flag mask with the overall result mask
    combine_flag_mask = combine_flag_mask | current_flag_mask
  return combine_flag_mask

# find window_size
def find_window_size(season, harvest_date):
    logger = logging.getLogger(__name__)
    try:
        date_obj = datetime.strptime(harvest_date, '%d-%m-%Y')
        doh = date_obj.strftime('%Y-%m-%d')  # date of harvest
        year = date_obj.year

        if season == "SA":
            window_size = f"{year}-04-01/{doh}"
            logger.info(f"Window size for SA: {window_size}")
        elif season == "WS":
            window_size = f"{year - 1}-11-01/{doh}"
            logger.info(f"Window size for WS: {window_size}")
        else:
            raise ValueError(f"Invalid season: {season}")

        return window_size

    except Exception as e:
        logger.error(f"Error in find_window_size ls: {e}", exc_info=True)
        raise

def get_ls_index(longitude, latitude, season, date, box_deg=0.10):
    logger = logging.getLogger(__name__)

    try:
        logger.info(
            f"Fetching LS index for Longitude: {longitude}, Latitude: {latitude}, Season: {season}, Date: {date}")
        catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

        time_range = find_window_size(season, date)
        logger.info(f"Time range: {time_range}")

        bbox = [
            longitude - box_deg / 2,
            latitude - box_deg / 2,
            longitude + box_deg / 2,
            latitude + box_deg / 2
        ]
        logger.info(f"BBox: {bbox}")

        # Search for relevant satellite images from the catalog
        search = catalog.search(
            collections=["landsat-c2-l2"],
            bbox=bbox,
            datetime=time_range,
            query={
                'eo:cloud_cover': {"lt": 10},  # 10% cloud cover
                'platform': {"in": ["landsat-8", "landsat-9"]}
            })
        items = search.get_all_items()
        if not items:
            logger.warning("No images found.")
            return np.nan, np.nan, np.nan, np.nan
        logger.info(f"Found {len(items)} images")

        # Select the image with the least cloud cover
        selected_item = min(items, key=lambda item: eo.ext(item).cloud_cover)
        bands_interest = ['red', 'nir08', 'qa_pixel', 'green', 'blue', 'swir16']

        xx = stac_load(
            [selected_item],
            bands=bands_interest,
            crs='EPSG:4326',
            resolution=30 / 111320,
            patch_url=pc.sign,
            bbox=bbox).isel(time=0)

        # Apply scaling and offset for the bands
        xx['red'] = xx['red'] * 0.0000275 - 0.2
        xx['nir08'] = xx['nir08'] * 0.0000275 - 0.2
        xx['green'] = xx['green'] * 0.0000275 - 0.2
        xx['blue'] = xx['blue'] * 0.0000275 - 0.2
        xx['swir16'] = xx['swir16'] * 0.0000275 - 0.2

        # Mask invalid data
        quality_mask = get_flags_to_mask(xx['qa_pixel'],
                                         ['fill', 'dilated_cloud', 'cirrus', 'cloud', 'shadow', 'water'])
        masked_data = xx.where(~quality_mask)
        clean_data = masked_data.mean(dim=['longitude', 'latitude']).compute()

        # Cleaned bands
        nir08 = clean_data.nir08.item()
        red = clean_data.red.item()
        green = clean_data.green.item()
        blue = clean_data.blue.item()
        swir16 = clean_data.swir16.item()

        # Calculate index
        ndvi = (nir08 - red) / (nir08 + red)
        ndwi = (green - swir16) / (green + swir16)
        avi = np.power((nir08 * (1 - red) * (nir08 - red)), 1 / 3)
        ndmi = (nir08 - swir16) / (nir08 + swir16)

        logger.info(f"Indices calculated - NDVI: {ndvi}, NDWI: {ndwi}, AVI: {avi}, NDMI: {ndmi}")
        return ndvi, ndwi, avi, ndmi

    except Exception as e:
        logger.error(f"Error in get_ls_index: {e}", exc_info=True)
        raise

# Test the function
# ndvi, ndwi, avi, ndmi = get_ls_index(105.248554,10.510542, "SA", "15-07-2023"  )
# print(ndvi, ndwi, avi, ndmi)

# date = find_window_size("WS", "15-07-2023")
# print(date)