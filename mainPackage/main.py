#main.py

from cleanedDataPackage.cleanedData import *
from dataAnomoliesPackage.dataAnomolies import *
import requests

if __name__ == "__main__":
    

    def get_zip_code(city, api_key):
        """
        Fetches the zip code for a given city using an external API.

        Parameters:
            city (str): The city name to query.
            api_key (str): The API key for the zip code service.

        Returns:
            str or None: The first zip code for the city if available, otherwise None.
        """
        try:
            response = requests.get(
                f"https://app.zipcodebase.com/api/v1/search?apikey={api_key}&city={city}",
                timeout=5  # Timeout for the request
            )
            response.raise_for_status()  # Raise an error for bad HTTP status codes
            zip_codes = response.json().get('results', {}).get(city, [])
            return zip_codes[0] if zip_codes else None
        except requests.exceptions.RequestException as e:
            print(f"Error fetching zip code for city '{city}': {e}")
            return None

    def main():
        """
        The main function that handles the complete data pipeline.

        Steps:
        1. Handle data anomalies.
        2. Clean the data using the provided API for zip codes.
        """
        # Define paths and API key
        api_key = '28e3c380-a0fc-11ef-9dfd-19e3353ada6e'
        input_csv = 'data/fuelPurchaseData.csv'

        # Step 1: Process anomalies
        anomaly_handler = AnomalyHandler(input_csv)
        anomaly_handler.process_anomalies()

        # Step 2: Clean data
        data_cleaner = DataCleaner(input_csv)
        data_cleaner.clean_data(api_key, get_zip_code)

    print("Starting the program")
    main()