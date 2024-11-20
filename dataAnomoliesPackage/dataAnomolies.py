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
