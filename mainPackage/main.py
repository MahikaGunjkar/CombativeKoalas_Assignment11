#main.py

from cleanedDataPackage.cleanedData import *
from dataAnomoliesPackage.dataAnomolies import *
import requests
import os
if __name__ == "__main__":
    print("Starting the program")

    # Ensure the data directory exists
    if not os.path.exists('data'):
        os.makedirs('data')

    # Define paths and API key
    api_key = '28e3c380-a0fc-11ef-9dfd-19e3353ada6e'
    input_csv = 'data/fuelPurchaseData.csv'
    cleaned_csv = 'data/cleanedFuelData.csv'

    # Step 1: Process anomalies
    anomaly_handler = AnomalyHandler(input_csv)
    print("Processing anomalies...")
    anomaly_handler.process_anomalies()

    # Step 2: Clean data
    data_cleaner = DataCleaner(cleaned_csv)
    print("Cleaning data...")
    data_cleaner.clean_data(api_key, get_zip_code)

    print("Program completed successfully.")