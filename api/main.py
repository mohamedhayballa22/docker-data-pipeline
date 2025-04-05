import json
import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from models import DataItem, DataResponse

app = FastAPI(title="Data Pipeline API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Data Pipeline API is running"}

@app.get("/data", response_model=DataResponse)
def get_data():
    data_path = "/app/data/transformed_data.json"
    
    if not os.path.exists(data_path):
        raise HTTPException(status_code=404, detail="Data not found. The transformer might still be processing.")
    
    try:
        with open(data_path, "r") as f:
            data = json.load(f)
        
        return DataResponse(
            items=[DataItem(**item) for item in data],
            count=len(data)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading data: {str(e)}")

@app.get("/health")
def health_check():
    return {"status": "healthy"}