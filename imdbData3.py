# Purpose: To scrape the data from IMDB website and store it in a database
from requests import get
from bs4 import BeautifulSoup
from time import sleep
from random import randint
import numpy as np
import psycopg2

db_params = {
    'dbname': 'imdbData',
    'user': 'postgres',
    'password': '12345',
    'host': 'localhost',
    'port': '5432'
}
conn = psycopg2.connect(**db_params)
c = conn.cursor()

c.execute('CREATE SCHEMA IF NOT EXISTS action_movies')
conn.commit()
c.execute('DROP TABLE IF EXISTS action_movies')

create_table_query = '''
    CREATE TABLE IF NOT EXISTS action_movies(
        id SERIAL PRIMARY KEY,
        movie TEXT,
        year INTEGER,
        rating TEXT,
        genre TEXT,
        runtime_min INTEGER,
        imdb FLOAT,
        metascore INTEGER,
        votes INTEGER
    )
'''
c.execute(create_table_query)
conn.commit()

pages = np.arange(1, 1051, 50)
headers = {'Accept-Language': 'en-US,en;q=0.8'}

for page in pages:
    response = get("https://www.imdb.com/search/title/?title_type=feature&genres=action&"
                   + "start="
                   + str(page)
                   + "&explore=title_type,genres&ref_=adv_prv", headers=headers)

    sleep(randint(8, 15))

    page_html = BeautifulSoup(response.text, 'html.parser')
    movie_containers = page_html.find_all('div', class_='lister-item mode-advanced')

    for container in movie_containers:
        if container.find('div', class_='ratings-metascore') \
                is not None:

            title = container.h3.a.text

            year = int(container.h3.find('span', class_='lister-item-year text-muted unbold').text[-5:-1])

            rating = container.p.find('span', class_='certificate').text \
                if container.p.find('span', class_='certificate') \
                else ""

            genre = container.p.find('span', class_='genre').text.replace("\n", "").rstrip()  \
                if container.p.find('span', class_='genre')  \
                else ""

            runtime = int(container.p.find('span', class_='runtime').text.replace(" min", "")) \
                if container.p.find('span', class_='runtime') \
                else None

            imdb = float(container.strong.text) \
                if container.strong \
                else None

            metascore = int(container.find('span', class_='metascore').text) \
                if container.find('span', class_='metascore') \
                else None

            vote = int(container.find('span', attrs={'name': 'nv'})['data-value']) \
                if container.find('span', attrs={'name': 'nv'}) \
                else None

            insert_query = '''INSERT INTO action_movies (movie, year, rating, genre, runtime_min, imdb, metascore, votes)
                              VALUES (%s, %s, %s, %s, %s, %s, %s, %s) '''

            c.execute(insert_query, (title, year, rating, genre, runtime, imdb, metascore, vote))
            conn.commit()

conn.close()
