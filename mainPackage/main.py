# Name: Mahika Gunjkar, Nandini Agrawal, Ishani Roy Chowdhury, Greyson Barber
# email:  gunjkamg@mail.uc.edu, Agarwand@mail.uc.edu, roychoii@mail.uc.edu, barbergn@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date:   11/21/2024
# Course #/Section:  4010- 001
# Semester/Year:   Fall 2024
# Brief Description of the assignment: In this assignment, we need to clean up the data in the provided CSV file. 

# Brief Description of what this module does. This module is running the functions to get all of the clean data.
# Citations:
# Anything else that's relevant : We did a bit research and got to know that because of the free accound we can only get upto 5000 credits worth data, as the file is running the 
# output is crashing after a certain point as the data is exceeding the limit. Also we realised that the API key does not work the moment we push something to github and it gets cancelled.

# main.py

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

    
    # Global dictionary to store city names and their corresponding first zip code
    city_zip_cache = {}
    i = 0
    def get_zip_code(city, api_key, max_retries=3):
        """
        Fetches the zip code for a given city using an external API or from the cache.

        Parameters:
            city (str): The city name to query.
            api_key (str): The API key for the zip code service.
            max_retries (int): Number of retry attempts in case of failure.

        Returns:
            str or None: The first zip code for the city if available, otherwise None.
        """
        # Check if the city is already in the cache
        city = city.lower()
        print(f"{i}. {city}")
        if city in city_zip_cache:
            print(f"Cache hit for city: {city}")
            return city_zip_cache[city]

        # If not in cache, fetch from the API
        url = f"https://app.zipcodebase.com/api/v1/code/city?apikey={api_key}&city={city}&country=us"
        retries = 0

        while retries < max_retries:
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                zip_codes = response.json().get('results', [])
                if zip_codes:
                    first_zip_code = zip_codes[0]
                    city_zip_cache[city] = first_zip_code  # Update the cache
                    return first_zip_code
                else:
                    return None
            except requests.exceptions.RequestException as e:
                retries += 1
                print(f"Error fetching zip code for city '{city}': {e} (Attempt {retries}/{max_retries})")
                logging.error(f"Error fetching zip code for city '{city}': {e}")
                time.sleep(2)  # Backoff before retrying

        # Return None if all retries fail
        return None

       

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