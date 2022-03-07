from decimal import Decimal
from pprint import pprint
import time
import array as arr

import json
import boto3


def load_movies(movies, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    start = time.time()
    table = dynamodb.Table('Phong_San_Movies_Python')
    flag = 0
    for movie in movies:
        flag = flag + 1
        data = {}
        year = int(movie['year'])
        title = movie['title']
        data['year'] = int(year)
        data['title'] = title
        data['directors'] = movie['info'].get('directors', None)
        data['release_date'] = movie['info'].get('release_date', None)
        data['rating'] = movie['info'].get('rating', None)
        data['genres'] = movie['info'].get('genres', None)
        data['image_url'] = movie['info'].get('image_url', None)
        data['plot'] = movie['info'].get('plot', None)
        data['rank'] = movie['info'].get('rank', None)
        data['running_time_secs'] = movie['info'].get('running_time_secs', None)
        data['actors'] = movie['info'].get('actors', None)
        data['status'] = 1

        print("Adding movie:", flag, year, title)
        table.put_item(Item=data)

    end = time.time()
    print("Total item: "  + str(flag) + "\n")
    print("Time run:")
    print(end - start)
    print("\n")

if __name__ == '__main__':
    with open("moviedata.json") as json_file:
        movie_list = json.load(json_file, parse_float=Decimal)
    load_movies(movie_list)