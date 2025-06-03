import os
import pandas as pd

# Path to the folder containing the Excel files
folder_path = r"PATH"

# Get a list of all Excel files in the folder
excel_files = [file for file in os.listdir(folder_path) if file.endswith('.xlsx')]

# Loop through each Excel file
for file_name in excel_files:
    # Read the Excel file into a DataFrame
    df = pd.read_excel(os.path.join(folder_path, file_name))
    
    # Extract the date from the file name
    date = file_name.split('_')[-1].split('.')[0]
    print(date)

    # Check if the 'Date' column already exists in the DataFrame
    if 'Date_Scraping' not in df.columns:
        # Add a new column with the numeric date value
        df['Date_Scraping'] = pd.to_numeric(date)
    else:
        print('Date column already created')
    
    # Write the updated DataFrame back to the Excel file
    df.to_excel(os.path.join(folder_path, file_name), index=False)

print("Columns added successfully!")