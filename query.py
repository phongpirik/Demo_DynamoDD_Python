from pprint import pprint
import boto3
import time

from boto3.dynamodb.conditions import Key

def scan_movies(year_range, display_movies, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('Phong_San_Movies_Python')
    scan_kwargs = {
        'FilterExpression': Key('year').between(*year_range),
        'ProjectionExpression': "#yr, title, rating",
        'ExpressionAttributeNames': {"#yr": "year"}
    }

    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        display_movies(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

def query_movies(year, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('Phong_San_Movies_Python')
    response = table.query(
        KeyConditionExpression=Key('year').eq(year)
    )
    return response['Items']

def query_and_project_movies(year, title_range, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('Phong_San_Movies_Python')
    print(f"Get year, title, genres, and lead actor")

    # Expression attribute names can only reference items in the projection expression.
    response = table.query(
        ProjectionExpression="#yr, title, genres, actors",
        ExpressionAttributeNames={"#yr": "year"},
        KeyConditionExpression=
            Key('year').eq(year) & Key('title').between(title_range[0], title_range[1])
    )
    return response['Items']

if __name__ == '__main__':
    
    def print_movies(movies):
            for movie in movies:
                print(f"\n{movie['year']} : {movie['title']}")
                pprint(movie)
            print("Total item: " + str(len(movies)))

    start = time.time()
    query_year = 2004
    query_range = ('A', 'N')
    print(f"Get movies from {query_year} with titles from "
          f"{query_range[0]} to {query_range[1]}")
    query_ranges = (1900, 2020)
    print(f"Scanning for movies released from {query_ranges[0]} to {query_ranges[1]}...")
    #scan_movies(query_ranges, print_movies)
    # movies = query_movies(query_year)
    # print_movies(movies)
    movies = query_and_project_movies(query_year, query_range)
    print_movies(movies)
    end = time.time()
    print("Time run:")
    print(end - start)
    print("\n")
