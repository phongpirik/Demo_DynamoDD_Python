from pprint import pprint
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

TbMovie = 'Phong_San_Movies_Python'

def put_movie(title, year, plot, rating,status, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(TbMovie)
    response = table.put_item(
       Item={
            'year': year,
            'title': title,
            'plot': plot,
            'rating': rating,
            'status': status
        }
    )
    return response

def get_movie(title, year, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(TbMovie)

    try:
        response = table.get_item(Key={'year': year, 'title': title})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

def update_movie(title, year, rating, plot, actors, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(TbMovie)

    response = table.update_item(
        Key={
            'year': year,
            'title': title
        },
        UpdateExpression="set rating=:r, plot=:p, actors=:a",
        ExpressionAttributeValues={
            ':r': Decimal(rating),
            ':p': plot,
            ':a': actors
        },
        ReturnValues="UPDATED_NEW"
    )
    return response

if __name__ == '__main__':
    #put
    # movie_resp = put_movie("The Big New Movie", 2015,"Nothing happens at all.", 0, 1)
    # print("Put movie succeeded:")

    # pprint(movie_resp, sort_dicts=False)

    #getmovie
    # movie = get_movie("The Big New Movie", 2015)
    # if movie:
    #     print("Get movie succeeded:")
    #     pprint(movie, sort_dicts=False)

    # #update 
    update_response = update_movie(
        "The Big New Movie", 2015, 5.5, "Everything happens all at once.",
        ["Larry", "Moe", "Curly"])
    print("Update movie succeeded:")
    pprint(update_response, sort_dicts=False)