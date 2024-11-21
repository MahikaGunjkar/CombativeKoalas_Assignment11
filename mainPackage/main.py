# Name: Mahika Gunjkar, Nandini Agrawal, Ishani Roy Chowdhury, Greyson Barber
# email:  gunjkamg@mail.uc.edu, Agarwand@mail.uc.edu, roychoii@mail.uc.edu, barbergn@mail.uc.edu
# Assignment Number: Assignment 1
# Due Date:   11/21/2024
# Course #/Section:  4010- 001
# Semester/Year:   Fall 2024
# Brief Description of the assignment: In this assignment, we need to clean up the data in the provided CSV file. 

# Brief Description of what this module does. This module is running the functions to get all of the clean data.
# Citations:
# Anything else that's relevant : We did a bit research and got to know that because of the free accound we can only get upto 5000 credits worth data, as the file is running the 
# output is crashing after a certain point as the data is exceeding the limit. Also we realised that the API key does not work the moment we push something to github and it gets cancelled.


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