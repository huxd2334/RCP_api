
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel,Field
from datetime import date
import numpy as np
import logging
import asyncio
from fastapi.middleware.cors import CORSMiddleware

from process_input.identify import get_vvvh_by_stage
from process_input.landsat_index import get_indices_by_stage
from process_input.sentinel1_index import get_rvi_by_stage
from process_input.weather import get_weather_data
# from process_input.sentinel2_index import fetch_and_process_data

import joblib

app = FastAPI()
# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    model = joblib.load("model/model_cb_rf.pkl")
    model2 = joblib.load("model/crop_classification_3.pkl")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    model = None
    model2 = None

class IdentifyRequest(BaseModel):
    Date_of_Harvest: date
    Latitude: float
    Longitude: float


class PredictionRequest(BaseModel):
    Date_of_Harvest: date
    Latitude: float
    Longitude: float
    Season: str
    Intensity: str

# def format_season(season: str) -> str:
    # season_mapping = {
    #     "Season(SA = Summer Autumn, WS = Winter Spring)_SA": "SA",
    #     "Season(SA = Summer Autumn, WS = Winter Spring)_WS": "WS"
    # }
    # return season_mapping.get(season, "Invalid Season")

def formmat_intensity(intensity: str) -> str:
    intensity_mapping = {
        "D": [0, 1],
        "T": [1, 0]
    }
    return intensity_mapping.get(intensity, "Invalid Rice Intensity")

def replace_nan_with_none(data):
    if isinstance(data, list):
        return [replace_nan_with_none(item) for item in data]
    elif isinstance(data, dict):
        return {key: replace_nan_with_none(value) for key, value in data.items()}
    elif isinstance(data, float) and np.isnan(data):
        return None
    else:
        return data
@app.get("/")
async def root():
    return {"message": "Hello World"}

# @app.post("/identify/")
# async def identify(data: IdentifyRequest):
#     try:
#         date_str = data.Date_of_Harvest.strftime('%d-%m-%Y')
#
#         # Run all fetch operations in parallel
#         vvvh_future = asyncio.to_thread(
#             get_vvvh_by_stage,
#             data.Latitude,
#             data.Longitude,
#             date_str,
#         )
#         # Wait for all operations to complete
#         vvvh_result = await asyncio.gather(
#             vvvh_future
#         )
#
#         # Unpack the results
#         (vh2_i, vv2_i), (vh_i, vv_i), (vh_2, vv_2), (vh, vv) = vvvh_result[0]
#
#         # Combine all features into a single array
#         features = np.array([
#             vh2_i, vv2_i, vh_i, vv_i,
#             vh_2, vv_2, vh, vv
#         ]).reshape(1, -1)
#
#         # Perform prediction using the model pipeline
#         land = model2.predict(features)
#         response = {
#             "class_of_land": land.tolist(),
#         }
#
#         response = replace_nan_with_none(response)
#         return response
#     except Exception as e:
#         logger.error(f"Prediction error: {e}", exc_info=True)
#         raise HTTPException(status_code=400, detail=f"Prediction error: {e}")


@app.post("/predict/")
async def predict(data: PredictionRequest):
    try:
        season = data.Season
        intensity = formmat_intensity(data.Intensity)
        date_str = data.Date_of_Harvest.strftime('%d-%m-%Y')

        # Run all fetch operations in parallel
        rvi_future = asyncio.to_thread(
            get_rvi_by_stage,
            data.Latitude,
            data.Longitude,
            date_str,
            season
        )

        indices_future = asyncio.to_thread(
            get_indices_by_stage,
            data.Latitude,
            data.Longitude,
            date_str,
            season
        )

        weather_future = asyncio.to_thread(
            get_weather_data,
            data.Longitude,
            data.Latitude,
            season,
            date_str
        )

        # Wait for all operations to complete
        rvi_result, indices_result, weather_result = await asyncio.gather(
            rvi_future,
            indices_future,
            weather_future
        )

        # Unpack the results
        rvi_1, rvi_2, rvi_3 = rvi_result

        # Unpack indices
        (ndvi_1, savi_1, ndwi_1, ndmi_1), \
            (ndvi_2, savi_2, ndwi_2, ndmi_2), \
            (ndvi_3, savi_3, ndwi_3, ndmi_3) = indices_result

        # Unpack weather data
        mean_humidity, mean_precip = weather_result

        # Combine all features into a single array
        features = np.array([
            ndvi_1, ndvi_2, ndvi_3,
            ndmi_1, ndmi_2, ndmi_3,
            rvi_1, rvi_2, rvi_3,
            ndwi_1, ndwi_2, ndwi_3,
            savi_1, savi_2, savi_3,
            mean_precip, mean_humidity, *intensity
        ]).reshape(1, -1)

        # Perform prediction using the model pipeline
        prediction = model.predict(features)
        response = {
            "predicted_crop_yield": prediction.tolist(),
            "rvi": [rvi_1, rvi_2, rvi_3],
            "ndvi": [ndvi_1, ndvi_2, ndvi_3],
            "savi": [savi_1, savi_2, savi_3],
            "ndwi": [ndwi_1, ndwi_2, ndwi_3],
            "ndmi": [ndmi_1, ndmi_2, ndmi_3],
            "mean_precip": mean_precip,
            "mean_humidity": mean_humidity
        }

        response = replace_nan_with_none(response)
        return response
    except Exception as e:
        logger.error(f"Prediction error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Prediction error: {e}")
# Run FastAPI server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="localhost", port=8000, reload=True)
