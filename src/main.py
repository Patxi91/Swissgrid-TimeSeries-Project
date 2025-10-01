

import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from typing import List
from pydantic import BaseModel
from datetime import datetime, timezone
import asyncio
import os
from enum import Enum

class TimeSeriesData(BaseModel):
    timestamp: datetime
    frequency: float
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "timestamp": "2021-05-02T10:00:00Z",
                    "frequency": 50.0
                }
            ]
        }
    }


# Allowed tables (whitelist)
class TableName(str, Enum):
    swissgrid_frequency_data = "swissgrid_frequency_data"
    volume_frequency_data = "volume_frequency_data"
    stresstest_frequency_data = "stresstest_frequency_data"

# Allowed resolutions (explicit whitelist, used as Swagger dropdown)
class Resolution(str, Enum):
    ms1 = "1ms"
    s1 = "1s"
    s15 = "15s"
    s30 = "30s"
    m1 = "1m"
    m5 = "5m"
    m10 = "10m"
    m15 = "15m"
    m30 = "30m"
    h1 = "1h"
    h6 = "6h"
    d1 = "1d"

db_pool = None

async def get_async_db_pool():
    """Tries to connect with retries if DB is not ready yet."""
    retries = 5
    delay = 5
    for attempt in range(1, retries + 1):
        try:
            return await asyncpg.create_pool(
                user=os.getenv("POSTGRES_USER", "swissgrid"),
                password=os.getenv("POSTGRES_PASSWORD", "swissgrid1234"),
                database=os.getenv("POSTGRES_DB", "timeseries_db"),
                host="db"
            )
        except Exception as e:
            print(f"DB connection attempt {attempt}/{retries} failed: {e}")
            if attempt == retries:
                raise
            await asyncio.sleep(delay)

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await get_async_db_pool()
    yield
    if db_pool:
        await db_pool.close()

app = FastAPI(title="Swissgrid Time Series API", lifespan=lifespan)

@app.get("/", tags=["Root"], response_class=HTMLResponse)
def read_root():
    """Returns a user-friendly welcome page with a logo and a link to the docs."""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Swissgrid API</title>
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
    </head>
    <body class="bg-gray-100 flex items-center justify-center min-h-screen">
        <div class="bg-white p-8 rounded-lg shadow-lg max-w-sm text-center">
            <img src="https://upload.wikimedia.org/wikipedia/commons/b/ba/Swissgrid_logo.svg" alt="Swissgrid Logo" class="mx-auto w-48 h-auto mb-4">
            <h1 class="text-3xl font-bold text-gray-800 mb-2">Swissgrid Time Series API</h1>
            <p class="text-gray-600 mb-6">
                Welcome to the time series API. Navigate to the documentation to explore the available endpoints.
            </p>
            <a href="/docs" class="inline-block bg-blue-600 text-white px-6 py-3 rounded-full text-lg font-semibold hover:bg-blue-700 transition duration-300">
                Access API
            </a>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/data/raw/{table_name}", response_model=List[TimeSeriesData], tags=["Query Data"])
async def get_raw_data(
    table_name: TableName, 
    start_time: str = Query(
        ...,
        description="Start of the time range (ISO 8601).",
        examples={
            "start": {
                "summary": "Example start time",
                "value": "2021-05-02T10:00:00Z"
            }
        }
    ),
    end_time: str = Query(
        ...,
        description="End of the time range (ISO 8601).",
        examples={
            "end": {
                "summary": "Example end time",
                "value": "2021-05-02T10:15:00Z"
            }
        }
    )
):
    """Get raw data from a specified table within a given time range."""
    clean_table_name = table_name.value
    
    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00')).astimezone(timezone.utc)
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00')).astimezone(timezone.utc)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use ISO 8601 (e.g., 2024-01-01T00:00:00Z)."
        )

    query = f"""
        SELECT timestamp, frequency
        FROM {clean_table_name}
        WHERE timestamp >= $1 AND timestamp <= $2
        ORDER BY timestamp;
    """
    
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch(query, start_dt, end_dt)
            if not records:
                raise HTTPException(
                    status_code=404,
                    detail="No data found for the specified time range."
                )
            return [dict(rec) for rec in records]
    except asyncpg.exceptions.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.get("/data/aggregated/{table_name}", response_model=List[TimeSeriesData], tags=["Query Data"])
async def get_aggregated_data(
    table_name: TableName,
    start_time: str = Query(
        ...,
        description="Start of the time range (ISO 8601).",
        examples={
            "start": {
                "summary": "Example start time",
                "value": "2021-05-02T10:00:00Z"
            }
        }
    ),
    end_time: str = Query(
        ...,
        description="End of the time range (ISO 8601).",
        examples={
            "end": {
                "summary": "Example end time",
                "value": "2021-05-02T10:15:00Z"
            }
        }
    ),
    resolution: Resolution = Query(Resolution.m15, description="Aggregation interval")
):
    """Get aggregated data from a specified table."""
    clean_table_name = table_name.value
    resolution_value = resolution.value

    try:
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00')).astimezone(timezone.utc)
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00')).astimezone(timezone.utc)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use ISO 8601."
        )

    # Interpolate resolution safely, since Enum restricts values
    query = f"""
        SELECT time_bucket('{resolution_value}'::interval, timestamp) AS bucket, AVG(frequency) AS frequency
        FROM {clean_table_name}
        WHERE timestamp >= $1 AND timestamp <= $2
        GROUP BY bucket
        ORDER BY bucket;
    """
    
    try:
        async with db_pool.acquire() as conn:
            records = await conn.fetch(query, start_dt, end_dt)
            if not records:
                raise HTTPException(
                    status_code=404, 
                    detail="No data found for the specified time range."
                )
            return [{"timestamp": rec["bucket"], "frequency": rec["frequency"]} for rec in records]
    except asyncpg.exceptions.PostgresError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
