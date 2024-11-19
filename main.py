
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import date
import numpy as np
import logging
import asyncio
import joblib

from process_input.landsat_index import get_ls_index
from process_input.sentinel1_index import get_rvi
from process_input.weather import get_weather_data

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    model = joblib.load("model/rf2_model.pkl")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    model = None

class PredictionRequest(BaseModel):
    Date_of_Harvest: date
    District: str
    Season: str
    Latitude: float
    Longitude: float
    Field_size_ha: float
    Intensity: str


def valid_harvest(season: str, harvest_date: date):
    if season == "Season(SA = Summer Autumn, WS = Winter Spring)_SA" and harvest_date.month not in [7, 8]:
        raise HTTPException(status_code=400, detail="For season 'SA', the month of Harvest must be July or August.")
    elif season == "Season(SA = Summer Autumn, WS = Winter Spring)_WS" and harvest_date.month not in [3, 4]:
        raise HTTPException(status_code=400, detail="For season 'WS', the month of Harvest must be March or April.")
    return True

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
        # Validate the harvest date and season
        valid_harvest(data.Season, data.Date_of_Harvest)
        season=format_season(data.Season)
        # Convert Date_of_Harvest to time-related features
        date_features = [
            data.Date_of_Harvest.year,  # Year
            (data.Date_of_Harvest.month - 1) // 3 + 1,  # Quarter
            data.Date_of_Harvest.month,  # Month
            data.Date_of_Harvest.timetuple().tm_yday,  # Day of Year
            data.Date_of_Harvest.day,  # Day of Month
            data.Date_of_Harvest.weekday(),  # Day of Week
            data.Date_of_Harvest.isocalendar()[1]
        ]

        # Map District and Season to numeric values or One-Hot Encoding
        district_mapping = {
            "District_Chau_Phu": [1, 0, 0],
            "District_Chau_Thanh": [0, 1, 0],
            "District_Thoai_Son": [0, 0, 1]
        }
        season_mapping = {
            "Season(SA = Summer Autumn, WS = Winter Spring)_SA": [1, 0],
            "Season(SA = Summer Autumn, WS = Winter Spring)_WS": [0, 1]
        }

        intensity_mapping = {
            'Rice Crop Intensity(D=Double, T=Triple)_D': [0, 1],
            'Rice Crop Intensity(D=Double, T=Triple)_T': [1, 0]
        }

        rvi= await asyncio.to_thread(get_rvi, data.Longitude, data.Latitude, season,
                                      data.Date_of_Harvest.strftime('%d-%m-%Y'))
        logger.info(f"RVI result: {rvi}")

        # Asynchronously fetch indices and weather data
        ndvi, ndwi, avi, ndmi = await asyncio.to_thread(get_ls_index, data.Longitude, data.Latitude, season,
                                                        data.Date_of_Harvest.strftime('%d-%m-%Y'))
        logger.info(f"LS index results - NDVI: {ndvi}, NDWI: {ndwi}, AVI: {avi}, NDMI: {ndmi}")

        precip = await asyncio.to_thread(get_weather_data, data.Longitude, data.Latitude, season,
                                         data.Date_of_Harvest.strftime('%d-%m-%Y'))
        logger.info(f"Weather data result - Precipitation: {precip}")


        district_encoded = district_mapping.get(data.District)
        if district_encoded is None:
            raise HTTPException(status_code=400, detail="Invalid District value.")

        season_encoded = season_mapping.get(data.Season)
        if season_encoded is None:
            raise HTTPException(status_code=400, detail="Invalid Season value.")

        intensity_encoded = intensity_mapping.get(data.Intensity)
        if intensity_encoded is None:
            raise HTTPException(status_code=400, detail="Invalid Intensity value.")

        # Combine all 9 features into a single array
        features = np.array([
            *date_features,
            *district_encoded,
            *season_encoded,
            data.Latitude,
            data.Longitude,
            data.Field_size_ha,
            *intensity_encoded,
            ndvi,
            ndwi,
            avi,
            ndmi,
            precip,
            rvi
        ]).reshape(1, -1)

        # Perform prediction using the model pipeline
        prediction = model.predict(features)

        # Return the prediction result
        return {"prediction_crop_yield": prediction.tolist()}



    except Exception as e:
        logger.error(f"Prediction error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Prediction error: {e}")
# Run FastAPI server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="localhost", port=8000, reload=True)
