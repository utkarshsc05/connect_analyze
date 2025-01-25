import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import mysql.connector
import os

DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASSWORD = "utkarsh"
DB_NAME = "investo"

def data_upload(excel_file, des_table):
    
    df = pd.read_excel(excel_file)
    df['datetime'] = pd.to_datetime(df['datetime'])

    connection = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = connection.cursor()

    insert_query = f"""
    INSERT INTO {des_table} (datetime, open, high, close, low, volume, instrument)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    for record in df.itertuples(index=False):
        cursor.execute(insert_query, record)

    connection.commit()
    cursor.close()
    connection.close()

    print("Excel data inserted successfully!")

def analyze(source_table, processed_table_name):

    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    query = f"SELECT * FROM {source_table};"
    df = pd.read_sql(query, engine)

    df.sort_values(by='datetime', inplace=True)

    df['SMA_5'] = df['close'].rolling(window=5).mean()
    df['SMA_20'] = df['close'].rolling(window=20).mean()

    df['signal'] = 0
    df.loc[df['SMA_5'] > df['SMA_20'], 'signal'] = 1
    df.loc[df['SMA_5'] < df['SMA_20'], 'signal'] = -1

    df['d_return'] = df['close'].pct_change()

    df['stra_return'] = df['signal'].shift(1) * df['d_return']

    df['cumm_stra_return'] = (1 + df['stra_return']).cumprod()
    df['cumm_market_return'] = (1 + df['d_return']).cumprod()

    df['stra_return'] = df['stra_return'].replace(-0, 0)


    df_filtered = df[['id', 'close', 'SMA_5', 'SMA_20', 'signal', 'd_return', 'stra_return', 'cumm_stra_return', 'cumm_market_return']]
    
    df_filtered.to_sql(processed_table_name, con=engine, if_exists='append', index=False)

    print(f"Processed data successfully saved to the table '{processed_table_name}'.")

path = os.getcwd()
file = "HINDALCO_1D.xlsx"
excel_file_path = os.path.join(path, file)
source_table_name = "data_table"
processed_table_name = "processed_data"
print(excel_file_path)

data_upload(excel_file_path, source_table_name)
analyze(source_table_name, processed_table_name)
