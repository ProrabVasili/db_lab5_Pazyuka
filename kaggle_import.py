import psycopg2
import matplotlib.pyplot as plt
import pandas as pd

username = 'pazyuka_oleg'
password = ''
database = 'restaurant_db'
host = 'localhost'
port = '5432'

CSV_FILE = 'zomato_dataset.csv'

def sampling_df():
  df = pd.read_csv('zomato_dataset.csv')

  df = df[(df['Restaurant Name'].str.len() < 30) & (df['Rating'] >= '1') & (df['Rating'] <= '5')]
  df = df.drop_duplicates()

  test_df = df.sample(10)

  return test_df

def insert_rest_cuisines(cur, rest_id, cuisn_ids):
    for cuisn_id in cuisn_ids:
        cur.execute(f'INSERT INTO restaurant_cuisine(restaurant_id, cuisine_id) VALUES ({rest_id}, {cuisn_id})')

insert_rest = lambda cur, rest_id, rest_name, rating, city_id: cur.execute('INSERT INTO restaurant(restaurant_id, restaurant_name, rating, city_id) VALUES (%s, %s, %s, %s)', (rest_id, rest_name, rating, city_id))    
insert_city = lambda cur, city_id, city: cur.execute(f'INSERT INTO location(city_id, city) VALUES ({city_id}, \'{city}\')')
insert_cuisine = lambda cur, cuisine_id, cuisine: cur.execute(f'INSERT INTO cuisine(cuisine_id, type_cuisine) VALUES ({cuisine_id}, \'{cuisine}\')')

df = sampling_df()

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
cur = conn.cursor()

cur.execute('SELECT * FROM cuisine')
cuisine_ids = {row[1]: row[0] for row in cur}
id_cuisine = {row[1]: row[0] for row in cuisine_ids.items()}
all_cuisines = cuisine_ids.keys()


cur.execute('SELECT * FROM location')
city_ids = {row[1]: row[0] for row in cur}
id_city = {row[1]: row[0] for row in city_ids.items()}
all_cities = city_ids.keys()


cur.execute('SELECT restaurant_id, restaurant_name, city_id FROM restaurant')
curr_cur = list(cur)
id_restaurant = {row[0]: row[1] for row in curr_cur}
restaurant_city = [(row[1], id_city[row[2]]) for row in curr_cur]


cur.execute('SELECT * FROM restaurant_cuisine')
restaurant_cuisines = [(id_restaurant[row[0]], id_cuisine[row[1]]) for row in cur]

for _, row in df.iterrows():
    rest_name, city = row['Restaurant Name'].replace("'", "\\'"), row['Location']
    
    if (rest_name, city) in restaurant_city:
        continue

    if city not in city_ids.keys():
        city_id = len(city_ids)+1
        city_ids[city] = city_id
        id_city[city_id] = city
        insert_city(cur, city_id, city)
        
    rest_id = len(id_restaurant) + 1
    city_id = city_ids[city]

    id_restaurant[rest_id] = rest_name
    restaurant_city.append((rest_name, city))
    
    insert_rest(cur, rest_id, rest_name, row['Rating'], city_id)
        
    cuisines = row['Cuisine'].split(', ')
    for cuisine in cuisines:
        if cuisine not in all_cuisines:
            cuisine_id = len(cuisine_ids)+1
            cuisine_ids[cuisine] = cuisine_id
            id_cuisine[cuisine_id] = cuisine
            insert_cuisine(cur, cuisine_id, cuisine)

    cuisn_ids = list(map(lambda x: cuisine_ids[x], cuisines))
    insert_rest_cuisines(cur, rest_id, cuisn_ids)
    

        
##conn.commit()    
    



