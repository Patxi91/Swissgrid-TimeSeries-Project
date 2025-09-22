from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert "Swissgrid Time Series API" in response.text

def test_docs():
    response = client.get("/docs")
    assert response.status_code == 200

def test_raw_data_invalid_date():
    response = client.get("/data/raw?start_time=invalid&end_time=invalid")
    assert response.status_code == 404
    assert "Not Found" in response.text
