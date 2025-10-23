# Muni-image-recognition

San Francisco Muni bus image recognition and interval forecasting. 


### Goal
<li> This project goal is to track Muni realtime data using local webcam by levaraging the YOLO Image recognition model. 


### The Data Pipeline
<li> Video is captured by a local webcam.
<li> An image recognition model captures when a new Muni arrives at the local stop.
<li> Results are parsed and streamed into a SQLite database.
<li> A forecast is provided based on trained data.
<li> Data is made available via Flask / HTML hosted on Google GCP. 