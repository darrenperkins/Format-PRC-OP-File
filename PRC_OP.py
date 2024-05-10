import os
import pandas as pd
import logging
import traceback
from datetime import datetime, timedelta
import numpy as np

# Set up logging
log_path = 'C:\\Log'
log_filename = 'script_log.txt'
log_filepath = os.path.join(log_path, log_filename)

logging.basicConfig(filename=log_filepath, level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# input folder - where postgresql CSV is saved to
folder_path = 'C:\\ToBeCleaned'
# output folder - where file is placed after cleaning
output_folder = 'C:\\CleanedFile'
# period in seconds to check the root folder for new file
period_to_check = 1800

files = os.listdir(folder_path) 

for file in files:
    if file.endswith('.csv'):
        csv_filename = file
        break
        
file_path = os.path.join(folder_path, csv_filename)

df = pd.read_csv(file_path)

df['Discharge/Service Date'] = pd.to_datetime(df['Discharge/Service Date'], errors='coerce')
today = datetime.now()

#Remove deceased patients
df = df[df['Discharge/Service Date'] != '0001-01-01']

df.dropna(subset=['Discharge/Service Date'], inplace=True)
df.dropna(subset=['CPT Codes'], inplace=True)
df.drop_duplicates(inplace=True)

# Drop rows where CPT Codes contain alphabetical character
df = df[~df['CPT Codes'].str.contains('[A-Za-z]')]

# Drop CPSI TEST Physician
df = df[df['Physician Name'] != 'CPSI TEST PHYSICIAN']
df = df[df['Physician Name'] != 'CPSI TEST']

# Convert Admit Source to integer to drop decimal
# df['Admit Source'] = df['Admit Source'].astype(int)

start_date = (datetime.now() - timedelta(days=14)).date()
end_date = (datetime.now() - timedelta(days=8)).date()

df = df[(df['Discharge/Service Date'].dt.date >= start_date) & (df['Discharge/Service Date'].dt.date <= end_date)]

df['original_index'] = df.index

# Sort by Account Code (Account Number)
df = df.sort_values(by='Account Code', ascending=False)

df = df.reset_index(drop=True)

df = df.drop(columns=['original_index'], errors='ignore')

# Convert Discharge/Service Date column to MM/DD/YYYY format
df['Discharge/Service Date'] = df['Discharge/Service Date'].dt.strftime('%m/%d/%Y')

# Group by 'Account Code' and aggregate other columns
grouped_df = df.groupby('Account Code').agg({
    'FileID': 'first',
    'Phone Number': 'first',
    'First Name': 'first',
    'Middle Name': 'first',
    'Last Name': 'first',
    'Mailing Address 1': 'first',
    'Mailing Address 2': 'first',
    'City': 'first',
    'State': 'first',
    'ZIP Code': 'first',
    'Date of Birth': 'first',
    'Gender': 'first',
    'Discharge/Service Date': 'first',
    'MRN': 'first',
    'Account Code': 'first',
    'Email Address': 'first',
    'Patient Language': 'first',
    'CCN': 'first',
    'System Name': 'first',
    'Hospital NPI': 'first',
    'HOPD/ASC Name': 'first',
    'Facility': 'first',
    'Facility Code': 'first',
    'Service Code': 'first',
    'Patient Type': 'first',
    'Service Description': 'first',
    'Treatment Type': 'first',
    'Admit Source': 'first',
    'Patient Discharge Status': 'first',
    'CPT Codes': lambda x: ' '.join(x),
    'Physician Name': 'first',
    'Physician NPI': 'first',
    'Room': 'first',
    'PRC Report Group': 'first',
    'EOR': 'first',
}).reset_index(drop=True)

# Fill null values with -1 then convert 'Physician NPI' to numeric. Drop rows with errors (alphanumeric characters). Convert to integer.

grouped_df['Physician NPI'] = grouped_df['Physician NPI'].replace(-1, np.nan)
grouped_df['Physician NPI'] = pd.to_numeric(grouped_df['Physician NPI'], errors='coerce')
grouped_df['Physician NPI'] = grouped_df['Physician NPI'].astype('Int64')

# Create function to trim decimal from Admit Source
def trim_decimal_admit_source(text):
    if pd.isnull(text):
        return text
    if isinstance(text, float):
        return str(int(text)) if text.is_integer() else str(text)
    if '.0' in text:
        return text[:text.rfind('.0')]
    return text

grouped_df['Admit Source'] = grouped_df['Admit Source'].apply(trim_decimal_admit_source)

# Function to trim decimal from MRN
def trim_decimal_mrn(text):
    if pd.isnull(text):
        return text
    if isinstance(text, float):
        text = str(text)  
    if isinstance(text, str):  
        if '.0' in text:
            return text[:text.rfind('.0')]
        return text
    return text  


grouped_df['MRN'] = grouped_df['MRN'].apply(trim_decimal_mrn)

# Append date & time to file name
current_datetime = datetime.now().strftime("%m%d%Y_%H%M%S")

# Create filename
csv_filename = f'C:\\ToSend\\OP_{current_datetime}.csv'


# Write dataframe to CSV
grouped_df.to_csv(csv_filename, index=False)


try:
	os.remove(file_path)
	print(f"File {file_path} deleted.")
except Exception as e:
	print(f"Error deleting file {file_path}: {e}")
	
