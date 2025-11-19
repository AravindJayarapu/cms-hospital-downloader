# CMS Hospital Data Downloader

Download and process all Hospital-related datasets from the CMS Provider Data Metastore.

## Features

- Converts CSV headers to snake_case
- Processes datasets in parallel
- Downloads only updated files since last run
- Tracks previous runs with run_metadata.json
- Saves cleaned data in 'cms_hospital_data/'

## Usage

1. Install dependencies:
    '''
    pip install -r requirements.txt
    '''
2. Run the script:
    '''
    python cms_hospital_downloader.py
    '''

## Automate Updates

Schedule with Task Scheduler (Windows) or cron (Linux) to run daily.


