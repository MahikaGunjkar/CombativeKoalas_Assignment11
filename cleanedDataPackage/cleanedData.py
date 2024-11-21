# Name: Mahika Gunjkar, Nandini Agrawal, Ishani Roy Chowdhury, Greyson Barber
# email:  gunjkamg@mail.uc.edu, Agarwand@mail.uc.edu, roychoii@mail.uc.edu, barbergn@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date:   11/21/2024
# Course #/Section:  4010- 001
# Semester/Year:   Fall 2024
# Brief Description of the assignment: In this assignment, we need to clean up the data in the provided CSV file. 

# Brief Description of what this module does. This module is making sure there are no anomolies within the fuel column. Ensuring that it's only fuel types, no Pepsi.
# Citations:
# Anything else that's relevant : We did a bit research and got to know that because of the free accound we can only get upto 5000 credits worth data, as the file is running the 
# output is crashing after a certain point as the data is exceeding the limit. Also we realised that the API key does not work the moment we push something to github and it gets cancelled

# cleanedData.py
import pandas as pd
import numpy as np
import re

class DataCleaner:
    def __init__(self, csv_path):
        self.csv_path = csv_path

    def format_gross_price(self, df):
        """
        Formats the Gross Price column to two decimal places.
        """
        df['Gross Price'] = df['Gross Price'].apply(lambda x: f"{float(x):.2f}")
        return df

    def parse_address(self, full_address):
        """
        Parses the full address into separate components: Address, City, Zip Code.
        """
        match = re.search(r"(.+),\s+([A-Za-z\s]+),?\s+OH\s*(\d{5})?", full_address)
        if match:
            address, city, zip_code = match.groups()
            return address.strip(), city.strip(), zip_code
        return full_address, None, None

    def fill_missing_zip_codes(self, df, api_key, get_zip_code_func):
        """
        Fills missing zip codes by calling the external API.
        """
        df[['Address', 'City', 'Zip Code']] = df['Full Address'].apply(
            lambda x: pd.Series(self.parse_address(x))
        )
        
        # Print the DataFrame before and after filling missing zip codes
        #print("Before filling missing zip codes:")
        #print(df.head())
        
        for idx, row in df[df['Zip Code'].isna()].iterrows():
            city = row['City']
            if city:
                zip_code = get_zip_code_func(city, api_key)
                if zip_code:
                    df.at[idx, 'Zip Code'] = zip_code

        #print("After filling missing zip codes:")
        #print(df.head())
        
        return df

    def clean_data(self, api_key, get_zip_code_func, batch_size=10):
        """
        Cleans the dataset by removing duplicates, formatting prices, and filling missing zip codes in batches.
        """
        df = pd.read_csv(self.csv_path, low_memory=False)
        df = df.drop_duplicates()
        df = self.format_gross_price(df)

        # Split the DataFrame into smaller batches
        num_batches = len(df) // batch_size + 1
        batch_dfs = np.array_split(df, num_batches)
        
        cleaned_batches = []
        for batch_df in batch_dfs:
            batch_df = self.fill_missing_zip_codes(batch_df, api_key, get_zip_code_func)
            cleaned_batches.append(batch_df)
        
        cleaned_df = pd.concat(cleaned_batches)

        # Save the DataFrame to CSV
        cleaned_df.to_csv('data/cleanedData.csv', index=False)
        print("Data successfully saved to data/cleanedData.csv")
