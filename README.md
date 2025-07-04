# Crop Yield Prediction API
A FastAPI-based backend for predicting crop yield using geospatial indices, satellite imagery, and weather data. The system integrates remote sensing data and machine learning to deliver accurate predictions.

### Features
- Predict crop yield based on geospatial and weather data
- Integrate satellite imagery from Sentinel-1, Sentinel-2, and Landsat
- Fetch real-time weather data for specific locations
- RESTful API powered by FastAPI, with interactive documentation

### Requirements
- Python 3.8+
- Dependencies listed in requirements.txt

### Installation
**1. Clone the repository**
```bash
git clone https://github.com/huxd2334/RCP_api.git
cd RCP_api
```
**2. Create a virtual environment and activate it**

```bash
python -m venv venv
source venv/bin/activate    # On Windows: venv\Scripts\activate
```
**3. Install project dependencies**

```bash
pip install -r requirements.txt
```


### Usage
**1. Start the FastAPI server:**

```bash
uvicorn main:app --host localhost --port 8000 --reload
```
**2. Access the API** (choose one of the following):
- Interactive Docs: http://localhost:8000/docs
- Postman 

**3. Endpoints:**

- ```POST /predict``` → Predict crop yield based on input data
   - Example Payload: 
      ```json
     {
        "Date_of_Harvest": "2024-12-23",
        "Season": "WS",
        "Latitude": 10.510542,
        "Longitude": 105.248554,
        "Intensity": "T"
      }
      ```
   - Notes
      - Date_of_Harvest: Start date of the crop YYYY-MM-DD format
     - Season: Crop season code ("WS" or "SA" for Winter-Spring or Summer-Autumn)
     - Latitude, Longitude: Geographic coordinates of the field
     - Number of crop yields in a year: ("T" or "D" for triple or double cropping)


- ```POST /predict_classification``` → Classify crop type (rice | non-rice)

### Project Structure
```
.
├── main.py                 # FastAPI application entry point
├── process_input/          # Data processing modules
│   ├── identify.py         # Handles satellite data processing
│   ├── landsat_index.py    # Processes Landsat vegetation indices
│   ├── sentinel1_index.py  # Processes Sentinel-1 vegetation indices
│   ├── weather.py          # Fetches and processes weather data
├── model/                  # Pre-trained machine learning models
├── requirements.txt        # Python dependency list
└── README.md               # Project documentation
```

### Acknowledgments
- [Microsoft Planetary Computer](https://planetarycomputer.microsoft.com/)  for satellite data
- [FastAPI](https://fastapi.tiangolo.com/) for the web framework

- All contributors and open-source projects that made this possible