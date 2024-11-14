#main.py

from cleanedDataPackage.cleanedData import *
from dataAnomoliesPackage.dataAnomolies import *
import requests

if __name__ == "__main__":
    print("Starting the program")

    def get_zip_code(city, api_key):
        # Perform the API request to get zip code
        response = requests.get(
            f"https://app.zipcodebase.com/api/v1/search?apikey={api_key}&city={city}"
        )
        if response.status_code == 200:
            zip_codes = response.json().get('results', {}).get(city, [])
            if zip_codes:
                return zip_codes[0]  # Pick the first zip code if multiple exist
        return None
 
    def main():
        # Define paths and API key
        input_csv = 'data/fuel_data.csv'
        api_key = 'YOUR_ZIPCODEBASE_API_KEY'
 
        # Step 1: Process anomalies
        anomaly_handler = AnomalyHandler(input_csv)
        anomaly_handler.process_anomalies()
 
        # Step 2: Clean data
        data_cleaner = DataCleaner(input_csv)
        data_cleaner.clean_data(api_key, get_zip_code)
 
