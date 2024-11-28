
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import date
import numpy as np
import logging
import asyncio

from process_input.landsat_index import process_landsat_data
from process_input.sentinel1_index import extract_sen1_data
from process_input.weather import get_weather_data
from process_input.sentinel2_index import fetch_and_process_data

import joblib

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    model = joblib.load("model/model.pkl")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    model = None


class PredictionRequest(BaseModel):
    Date_of_Harvest: date
    Latitude: float
    Longitude: float
    Season: str

def format_season(season: str) -> str:
    season_mapping = {
        "Season(SA = Summer Autumn, WS = Winter Spring)_SA": "SA",
        "Season(SA = Summer Autumn, WS = Winter Spring)_WS": "WS"
    }
    return season_mapping.get(season, "Invalid Season")
@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/predict/")
async def predict(data: PredictionRequest):
    try:
        season = format_season(data.Season)
        rvi_mean= \
            await asyncio.to_thread(extract_sen1_data, data.Longitude, data.Latitude,
                                      data.Date_of_Harvest.strftime('%d-%m-%Y'))
        # Asynchronously fetch indices and weather data
        mean_ndvi, mean_ndwi, mean_ndmi, mean_albedo= \
            await asyncio.to_thread(process_landsat_data, data.Date_of_Harvest.strftime('%d-%m-%Y'), data.Longitude, data.Latitude, )
        mean_humidity, mean_precip\
            = await asyncio.to_thread(get_weather_data, data.Longitude, data.Latitude, season,
                                         data.Date_of_Harvest.strftime('%d-%m-%Y'))
        mean_lai = await asyncio.to_thread(fetch_and_process_data, data.Longitude, data.Latitude,
                                      data.Date_of_Harvest.strftime('%d-%m-%Y'))

        # Combine all features into a single array
        features = np.array([
             mean_ndvi, mean_ndwi, mean_ndmi, mean_albedo,
            mean_lai,
            rvi_mean,
            mean_precip, mean_humidity
        ]).reshape(1, -1)

        # Perform prediction using the model pipeline
        prediction = model.predict(features)
        return {"predicted_crop_yield": prediction.tolist()}
    except Exception as e:
        logger.error(f"Prediction error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Prediction error: {e}")
# Run FastAPI server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="localhost", port=8000, reload=True)
