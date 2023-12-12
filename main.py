import psycopg2
import matplotlib.pyplot as plt

SHOW = 1

username = 'pazyuka_oleg'
password = ''
database = 'restaurant_db'
host = 'localhost'
port = '5432'

query_1 = '''
CREATE VIEW AvgRatingByCity AS
SELECT city, ROUND(AVG(rating) :: NUMERIC, 2) AS avg_rating
FROM restaurant
JOIN location USING (city_id)
GROUP BY city;
'''

query_2 = '''
CREATE VIEW NRestaurantsByCuisine AS 
SELECT type_cuisine, COUNT(*)
FROM restaurant
JOIN restaurant_cuisine USING(restaurant_id)
JOIN cuisine USING (cuisine_id)
GROUP BY type_cuisine
'''

query_3 = '''
CREATE VIEW UniqueCuisineByCity AS 
SELECT city, COUNT(DISTINCT cuisine_id)
FROM restaurant
JOIN restaurant_cuisine USING(restaurant_id)
JOIN location USING(city_id)
GROUP BY city;
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    figure, (bar1_ax, pie_ax, bar2_ax) = plt.subplots(1, 3, figsize=(30, 10))
    cur = conn.cursor()

    cur.execute('DROP VIEW IF EXISTS AvgRatingByCity')
    cur.execute(query_1)
    cur.execute('SELECT * FROM AvgRatingByCity;')

    cities = []
    ratings = []
    for row in cur:
        cities.append(row[0])
        ratings.append(float(row[1]))
        
    x_range = range(len(cities))
 
    bar1_ax.bar(x_range, ratings)
    bar1_ax.set_title('Average rating of restaurants by city')
    bar1_ax.set_xlabel('Cities')
    bar1_ax.set_ylabel('Average rating')
    bar1_ax.set_xticks(x_range)
    bar1_ax.set_xticklabels(cities, rotation=90)
    bar1_ax.set_yticks([i/2 for i in range(int(max(ratings)+1)*2)])
    
    cur.execute('DROP VIEW IF EXISTS NRestaurantsByCuisine')
    cur.execute(query_2)
    cur.execute('SELECT * FROM NRestaurantsByCuisine;')

    cuisines = []
    nums = []
    for row in cur:
        cuisines.append(row[0])
        nums.append(int(row[1]))
        
    patches, labels, pct_texts = pie_ax.pie(nums, labels=cuisines, autopct='%1.2f%%', rotatelabels=True, textprops={'fontsize': 7}, radius = 1.25)
    for label, pct_text in zip(labels, pct_texts):
        pct_text.set_rotation(label.get_rotation())
    pie_ax.set_title('Share of each type of cuisine', loc='right')

    cur.execute('DROP VIEW IF EXISTS UniqueCuisineByCity')
    cur.execute(query_3)
    cur.execute('SELECT * FROM UniqueCuisineByCity;')

    cities = []
    nums = []
    for row in cur:
        cities.append(row[0])
        nums.append(int(row[1]))
        
    x_range = range(len(cities))
 
    bar2_ax.bar(x_range, nums)
    bar2_ax.set_title('Number of unique cuisine types by city')
    bar2_ax.set_xlabel('Cities')
    bar2_ax.set_ylabel('Num of unique cuisine types')
    bar2_ax.set_xticks(x_range)
    bar2_ax.set_yticks(range(max(nums)+2))
    bar2_ax.set_xticklabels(cities, rotation=90)
     


mng = plt.get_current_fig_manager()
mng.resize(1400, 600)

if SHOW:
    plt.show()
else:
    plt.savefig('plots.png', bbox_inches='tight')

