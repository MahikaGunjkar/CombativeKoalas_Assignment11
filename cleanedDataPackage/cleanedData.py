import pandas as pd

class DataCleaner:
    def __init__(self, csv_path):
        self.csv_path = csv_path

    def format_gross_price(self, df):
        # Format the Gross Price to 2 decimal places
        df['Gross Price'] = df['Gross Price'].apply(lambda x: f"{float(x):.2f}")
        return df

    def fill_missing_zip_codes(self, df, api_key, get_zip_code_func):
        # Find rows with missing zip codes and fill them
        missing_zips = df[df['Zip Code'].isna()]
        
        for idx, row in missing_zips.iterrows():
            city = row['City']
            zip_code = get_zip_code_func(city, api_key)
            if zip_code:
                df.at[idx, 'Zip Code'] = zip_code
            
        return df

    def clean_data(self, api_key, get_zip_code_func):
        # Load the CSV file
        df = pd.read_csv(self.csv_path)

        # Step 1: Remove duplicates
        df = df.drop_duplicates()

        # Step 2: Format Gross Price
        df = self.format_gross_price(df)

        # Step 3: Fill missing zip codes
        df = self.fill_missing_zip_codes(df, api_key, get_zip_code_func)

        # Save cleaned data to cleanedData.csv
        df.to_csv('data/cleanedData.csv', index=False)
