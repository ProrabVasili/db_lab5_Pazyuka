import psycopg2
import pandas as pd


username = 'pazyuka_oleg'
password = ''
database = 'restaurant_db'
host = 'localhost'
port = '5432'
PATH_FOLDER = 'csv_files'

def get_csv(cur, tablename):
    cur.execute(f'SELECT * FROM {tablename}')
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(columns=colnames)
    for i, row in enumerate(cur):
        df.loc[i] = row

    df.to_csv(f'{PATH_FOLDER}/{tablename}.csv')
    
conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
cur = conn.cursor()

tables = ['location', 'cuisine', 'restaurant', 'restaurant_cuisine']

for tablename in tables:
    get_csv(cur, tablename)
