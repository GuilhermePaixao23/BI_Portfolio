import os
import pandas as pd

# Path to the folder containing the CSV files
folder_path = r"PATH"

# Get a list of all CSV files in the folder
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

# Loop through each CSV file
for file_name in csv_files:
    file_path = os.path.join(folder_path, file_name)
    
    try:
        # Read the first row of the CSV file
        first_row = pd.read_csv(file_path, delimiter=';', encoding='latin1', nrows=1, engine='python')
        
        # Check if the first cell contains the text "DATE_MOVE"
        if first_row.columns[0] == "DATE_MOVE":
            print(f"File {file_name} already processed.")
        else:
            print(f"Processing file: {file_name}")
            
            # Read CSV file into pandas DataFrame
            df = pd.read_csv(file_path, delimiter=';', encoding='latin1', skiprows=6, skipfooter=1, engine='python')
            
            # Select only columns up to the "H" column
            df = df.iloc[:, :8]

            # Print the first few rows of the DataFrame
            print(df.head())

            # Rename columns
            df = df.rename(columns={df.columns[0]: 'DATE_MOVE', df.columns[1]: 'DATE_VALUE',
                                    df.columns[2]: 'DSC', df.columns[3]: 'DEBIT', 
                                    df.columns[4]: 'CREDIT', df.columns[5]: 'ACCOUNTING_BALANCE', 
                                    df.columns[6]: 'AVAILABLE_BALANCE', df.columns[7]: 'CAT'})

            # Replace "." with "" in the numeric columns
            numeric_columns = ['DEBIT', 'CREDIT', 'ACCOUNTING_BALANCE', 'AVAILABLE_BALANCE']
            df[numeric_columns] = df[numeric_columns].replace('\.', '', regex=True)

            print(df.head())

            # Replace NaN values with None
            df = df.where(pd.notnull(df), None)

            print(df.head())

            # Create a month_key using the first value in the DATE_MOVE column
            date_sample = df['DATE_MOVE'].iloc[0].strip()  # Retrieve and strip any leading/trailing spaces
            month_key = date_sample[-4:] + "_" + date_sample[3:5]  # Format as "YYYY_MM"

            new_filename = f"Spendings_{month_key}.csv"
            new_file_path = os.path.join(folder_path, new_filename)
            print(new_file_path)

            df.to_csv(new_file_path, index=False, sep=';', encoding='latin1')
            print(f"File saved as {new_filename}")
            os.remove(file_path)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

print("Files ready for takeoff!")