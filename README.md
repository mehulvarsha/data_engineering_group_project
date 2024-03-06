# MSIN0166 Data Engineering 23/24 - Group Data Engineering Project

## Data Engineering Project on Data Collection, Processing, and Storage for Business Analytics
### Project Overview
This data engineering project aims prepare YouTube trending video data for future analysis on identifying high-quality content that aligns with current user interests. Utilising datasets from Kaggle and real-time data scraping from YouTube API, we developed a pipeline for data collection, processing, storage, and analysis, adhering to best industry practices.

### Repository Contents
- `scraper.py` - Python script for scraping current trending video data from YouTube.
- `scraper2.py` - Python script for scraping YouTube video categories.
- `api_key.txt` - Contains the API key for accessing the YouTube Data API.
- `country_codes.txt` - List of country codes used for scraping data specific to each region.
- `db_youtube_etl_csv_to_parquet.py` - ETL script for processing and transforming CSV data into Parquet format.
- `lambda_function_json_to_parquet.py` - AWS Lambda function for converting JSON category data into Parquet format.
- `db_youtube_parquet_analytics_version.py` - Script for analysing the processed Parquet data, focusing on video analytics.

### Project Structure
This project is structured to follow the steps of data collection, processing, and analysis:
1. **Data Collection:** Utilises `scraper.py` and `scraper2.py` to collect real-time data from YouTube, along with an API key stored in `api_key.txt` and country codes from `country_codes.txt`.
2. **Data Processing and Storage:** Employs AWS services for ETL processes, transforming data into a more efficient format (`db_youtube_etl_csv_to_parquet.py` and `lambda_function_json_to_parquet.py`).
3. **Data Analysis:** Analyses the cleaned and transformed data to extract valuable insights (`db_youtube_parquet_analytics_version.py`).
