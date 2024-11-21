# Name: Mahika Gunjkar, Nandini Agrawal, Ishani Roy Chowdhury, Greyson Barber
# email:  gunjkamg@mail.uc.edu, Agarwand@mail.uc.edu, roychoii@mail.uc.edu, barbergn@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date:   11/21/2024
# Course #/Section:  4010- 001
# Semester/Year:   Fall 2024
# Brief Description of the assignment: In this assignment, we need to clean up the data in the provided CSV file. 

# Brief Description of what this module does. This module is making sure there are no anomolies within the fuel column. Ensuring that it's only fuel types, is Pepsi.
# Citations: Used chat gpt
# Anything else that's relevant : We have added dictionary such that our code runs and functions even if the api key fails
                                 # The api key is getting terminated once we are pushing it to github
                                  # ZipCode is in Cleandata.csv
                                 # other changes in cleanfueldata.csv
# dataAnomolies.py
import pandas as pd

class AnomalyHandler:
    def __init__(self, csv_path):
        self.csv_path = csv_path
 
    def process_anomalies(self):
        df = pd.read_csv(self.csv_path, low_memory=False)
        anomalies = df[df['Fuel Type'] == 'Pepsi']  # Rows with Pepsi
        anomalies.to_csv('data/dataAnomalies.csv', index=False)  # Save anomalies
        # Filter out anomalies and retain only relevant rows
        self.cleaned_df = df[df['Fuel Type'] != 'Pepsi']
        self.cleaned_df.to_csv('data/cleanedFuelData.csv', index=False)  # Save cleaned data
