import subprocess
import os
import pandas as pd
import pyodbc

# Directory containing CSV files
directory = r"PATH"

run_file_treat_step = subprocess.run(
    ["python",r"PATH"],
    capture_output=True,
    text=True,
    check=True,
    timeout=10000
)
#This part of the code "subprocess" allows me to run the first script "File_treatment" in this script

output_file_treat_step = run_file_treat_step.stdout
print(output_file_treat_step)

# SQL Server connection details
SERVER = '----\SQLEXPRESS'
DATABASE = 'HPL'
USERNAME = '-------'
PASSWORD = '-------'

connection_string = f'Driver={{SQL Server}};' \
                    f'Server={SERVER};' \
                    f'Database={DATABASE};' \
                    f'UID={USERNAME};' \
                    f'PWD={PASSWORD};'

# Connect to SQL Server
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Truncate the table before inserting new data if needed
cursor.execute("TRUNCATE TABLE [HPL].[dbo].[Idealista_PY]")
conn.commit()

# Iterate over CSV files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".xlsx"):
        filepath = os.path.join(directory, filename)
        
        # Read CSV file into pandas DataFrame
        df = pd.read_excel(filepath, usecols=range(7))
        
        #No null treatment
        df = df.where(pd.notnull(df), None)

        print(df.head())

        # Insert data into SQL Server table
        for row in df.itertuples():
            cursor.execute('''
                INSERT INTO Idealista_PY ([Header], [Price], [Rooms], [Area], [Floor], [Link], [Date_Scraping])
                VALUES (?,?,?,?,?,?,?)
                ''',
                row.Header,
                row.Price,
                row.Rooms,
                row.Area,
                row.Floor,
                row.Link,
                row.Date_Scraping
            )
            conn.commit()
        
        conn.commit()

# Close connection
conn.close()

print('success')