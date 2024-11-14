#data Anomolies

import pandas as pd
 
class AnomalyHandler:
    def __init__(self, csv_path):
        self.csv_path = csv_path
 
    def process_anomalies(self):
        # Load data and identify anomalies
        df = pd.read_csv(self.csv_path)
        anomalies = df[df['Product'] == 'Pepsi']  # Rows with Pepsi
 
        # Save anomalies to dataAnomalies.csv
        anomalies.to_csv('data/dataAnomalies.csv', index=False)
        # Filter out anomalies and retain only relevant rows
        self.cleaned_df = df[df['Product'] != 'Pepsi']




