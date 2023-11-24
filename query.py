import sqlite3
from datetime import datetime

def query_logs(query):
    ''' This function logs the mentioned queries. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open('query_logs.txt', 'a') as f:
        f.write(timestamp + "," + query + '\n')
def run_query(query_statement):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    ''' Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''
    # Connect to the SQLite database
    sql_connection = sqlite3.connect('bank.db')
    # Create a cursor object to execute SQL queries
    cursor = sql_connection.cursor()

    # Execute the query
    cursor.execute(query_statement)
    # Fetch all the rows returned by the query
    rows = cursor.fetchall()
    # Process the rows
    for row in rows:
        print(row)
    # Close the cursor and the database connection
    cursor.close()
    sql_connection.close()
    query_logs(query_statement)


# Define the SQL query
query = "SELECT avg(MC_INR_Billion) FROM MarketCap"
run_query(query)
