import os
import requests
from cleanedDataPackage.cleanedData import *
from dataAnomoliesPackage.dataAnomolies import *
import time
import logging


if __name__ == "__main__":

    # Configure logging
    logging.basicConfig(
        filename='data_cleaning.log',
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Example fallback zip code dictionary
    FALLBACK_ZIP_CODES = {
        "Lynchburg": "45142",
        "Clifton": "45316",
        "Cleveland": "44101",
        "Bethel": "45106",
        "Circleville": "43113"
    }

    def get_zip_code(city, api_key, max_retries=3):
        """
        Fetches the zip code for a given city using an external API or fallback.

        Parameters:
            city (str): The city name to query.
            api_key (str): The API key for the zip code service.
            max_retries (int): Number of retry attempts in case of failure.

        Returns:
            str or None: The first zip code for the city if available, otherwise None.
        """
        url = f"https://app.zipcodebase.com/api/v1/code/city?apikey={api_key}&city={city}&country=us"
        retries = 0

        while retries < max_retries:
            try:
                response = requests.get(url, timeout=5)
                print(response)
                response.raise_for_status()
                zip_codes = response.json().get('results', {})
                print(zip_codes)
                return zip_codes[0] if zip_codes else None
            except requests.exceptions.RequestException as e:
                retries += 1
                print(f"Error fetching zip code for city '{city}': {e} (Attempt {retries}/{max_retries})")
                logging.error(f"Error fetching zip code for city '{city}': {e}")
                time.sleep(2)  # Backoff before retrying

        print(f"Using fallback for city '{city}'.")
        return FALLBACK_ZIP_CODES.get(city, None)

    # Define paths and API key
    api_key = "" 
    input_csv = 'data/fuelPurchaseData.csv'

    # Step 1: Process anomalies
    print("Starting the program")
    print("Processing anomalies...")
    anomaly_handler = AnomalyHandler(input_csv)
    anomaly_handler.process_anomalies()

    # Step 2: Clean data
    print("Cleaning data...")
    data_cleaner = DataCleaner(input_csv)
    data_cleaner.clean_data(api_key, get_zip_code)

    print("Data cleaning completed.")