# Use an official lightweight Python image.
FROM python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy local code to the container image
COPY . .

# Run the web service on container startup.
# Gunicorn is a production-ready WSGI server.
# Cloud Run will set the $PORT environment variable for us.
CMD exec gunicorn --bind :$PORT app:app
