import subprocess
import os
import pandas as pd
import pyodbc

# Directory containing CSV files
directory = r"Path"

correr_egp = subprocess.run(
    ["python",r"Path"],
    capture_output=True,
    text=True,
    check=True,
    timeout=10000,
    )

# SQL Server connection details
SERVER = '----\SQLEXPRESS'
DATABASE = 'CONTAS'
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

# Truncate the table before inserting new data
cursor.execute("TRUNCATE TABLE [CONTAS].[dbo].[CONTAS_CGD]")
conn.commit()

# Iterate over CSV files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        filepath = os.path.join(directory, filename)
        
        # Read CSV file into pandas DataFrame
        df = pd.read_csv(filepath, delimiter=';', encoding='latin1')

        #data_cleaning

        df = df.rename(columns={df.columns[0]: 'DATE_MOVE', df.columns[1]: 'DATE_VALUE',df.columns[2]: 'DSC',df.columns[3]: 'DEBIT',df.columns[4]: 'CREDIT',df.columns[5]: 'ACCOUNTING_BALANCE',df.columns[6]: 'AVAILABLE_BALANCE',df.columns[7]: 'CAT'})
        df['DATE_MOVE'] = pd.to_datetime(df['DATE_MOVE'], dayfirst=True).dt.strftime('%Y%m%d')
        df['DATE_VALUE'] = pd.to_datetime(df['DATE_VALUE'], dayfirst=True).dt.strftime('%Y%m%d')
        df = df.where(pd.notnull(df), None)

        Numeric_columns_to_0 = ['DEBIT', 'CREDIT', 'ACCOUNTING_BALANCE', 'AVAILABLE_BALANCE']
        for col in Numeric_columns_to_0:
            df[col] = df[col].str.replace(',', '.').astype(float).fillna(0)
        
        print(df.head())

        # Insert data into SQL Server table
        for row in df.itertuples():
            cursor.execute('''
                INSERT INTO CONTAS_CGD ([DATE_MOVE], [DATE_VALUE], [DSC], [DEBIT], [CREDIT], [ACCOUNTING_BALANCE], [AVAILABLE_BALANCE], [CAT])
                VALUES (?,?,?,?,?,?,?,?)
                ''',
                row.DATE_MOVE,
                row.DATE_VALUE,
                row.DSC,
                row.DEBIT,
                row.CREDIT,
                row.ACCOUNTING_BALANCE,
                row.AVAILABLE_BALANCE,
                row.CAT
            )
        
        conn.commit()

# Close connection
conn.close()

print('success')