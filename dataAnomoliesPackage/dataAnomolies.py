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
# output is crashing after a certain point as the data is exceeding the limit. Also we realised that the API key does not work the moment we push something to github and it gets cancelled.

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
