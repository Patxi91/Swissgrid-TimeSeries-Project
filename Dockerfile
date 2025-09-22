# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Run tests
COPY src/ ./src/
COPY tests/ ./tests/
#RUN PYTHONPATH=/app pytest --maxfail=1 --disable-warnings --tb=short

# Expose the port the app runs on
EXPOSE 8000

# Run the application
WORKDIR /app/src
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
