import os
import requests
import requests
from cleanedDataPackage.cleanedData import *
from dataAnomoliesPackage.dataAnomolies import *


if __name__ == "__main__":
    print("Starting the program")

    # Ensure the data directory exists
    if not os.path.exists('data'):
        os.makedirs('data')

    # Define paths and API key
    api_key = '28e3c380-a0fc-11ef-9dfd-19e3353ada6e'
    input_csv = 'data/fuelPurchaseData.csv'
    cleaned_csv = 'data/cleanedFuelData.csv'

    # Define the function for getting zip codes
    def get_zip_code(city, api_key):
        """
        Fetches the zip code for a given city using an external API.
        """
        try:
            response = requests.get(
                f"https://app.zipcodebase.com/api/v1/search?apikey={api_key}&city={city}",
                timeout=5  # Timeout for the request
            )
            response.raise_for_status()
            zip_codes = response.json().get('results', {}).get(city, [])
            return zip_codes[0] if zip_codes else None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching zip code for city '{city}': {e}")
            return None

    # Step 1: Process anomalies
    anomaly_handler = AnomalyHandler(input_csv)
    print("Processing anomalies...")
    anomaly_handler.process_anomalies()

    # Step 2: Clean data
    data_cleaner = DataCleaner(cleaned_csv)
    print("Cleaning data...")
    data_cleaner.clean_data(api_key, get_zip_code)

    print("Program completed successfully.")