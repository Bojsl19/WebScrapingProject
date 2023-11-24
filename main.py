# Code for ETL operations on Country-GDP data
# Importing the required libraries
import sqlite3
import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# URL for extract data
url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_csv_path = "exchange_rate.csv"
connection = sqlite3.connect('bank.db')

# create data frame
table_attribs = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"])


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open('logs.txt', 'a') as f:
        f.write(timestamp + "," + message + '\n')



def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_data = requests.get(url).text
    soup = BeautifulSoup(html_data, "html.parser")

    for row in soup.find_all('tbody')[0].find_all('tr'):
        col = row.find_all('td')
        # Write your code here
        if len(col) > 0:
            name = col[1].text.strip()
            market_cap = float(col[2].string.strip())
            table_attribs = table_attribs._append({"Name": name, "Market Cap (US$ Billion)": market_cap},ignore_index=True)

    return table_attribs


def transform(df, exchange_rate_csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    df.rename(columns={'Market Cap (US$ Billion)': 'MC_USD_Billion'}, inplace=True)
    exchange_rate_file = pd.read_csv(exchange_rate_csv_path)
    dict = exchange_rate_file.set_index('Currency').to_dict()['Rate']
    df['MC_INR_Billion'] = [np.round(x * dict['INR'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x * dict['GBP'], 2) for x in df['MC_USD_Billion']]

    return df


# ------------------------------------------------------------------------------------------------------------------
def load_to_csv(df):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv("dataframe_output.csv")


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, con=sql_connection, if_exists='replace', index=False)

    sql_connection.close()



print("*" * 1000)
log_progress("Start extract")
table_attribs = extract(url, table_attribs)
log_progress("end extract data")

log_progress("Start Transformation")
transform(table_attribs, exchange_rate_csv_path)
log_progress("end transform data")

log_progress('Start load data to csv file')
load_to_csv(table_attribs)
log_progress('end load data to csv file')

log_progress("Start to load data to database")
load_to_db(table_attribs, connection, 'MarketCap')
log_progress('end load data to database')

