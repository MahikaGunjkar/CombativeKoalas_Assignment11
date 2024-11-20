import pandas as pd
import requests
import re

class DataCleaner:
    def __init__(self, csv_path):
        self.csv_path = csv_path

    def format_gross_price(self, df):
        df['Gross Price'] = df['Gross Price'].apply(lambda x: f"{float(x):.2f}")
        return df

    def parse_address(self, full_address):
        match = re.search(r"(.+),\s+([A-Za-z\s]+),?\s+OH\s*(\d{5})?", full_address)
        if match:
            address, city, zip_code = match.groups()
            return address.strip(), city.strip(), zip_code
        return full_address, None, None

    def fill_missing_zip_codes(self, df, api_key, get_zip_code_func):
        df[['Address', 'City', 'Zip Code']] = df['Full Address'].apply(
            lambda x: pd.Series(self.parse_address(x))
        )
        
        for idx, row in df[df['Zip Code'].isna()].iterrows():
            city = row['City']
            if city:
                zip_code = get_zip_code_func(city, api_key)
                if zip_code:
                    df.at[idx, 'Zip Code'] = zip_code
            
        return df

    def clean_data(self, api_key, get_zip_code_func):
        df = pd.read_csv(self.csv_path, low_memory=False)

        df = df.drop_duplicates()
        df = self.format_gross_price(df)
        df = self.fill_missing_zip_codes(df, api_key, get_zip_code_func)
        df.to_csv('data/cleanedData.csv', index=False)


def get_zip_code(city, api_key):
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
