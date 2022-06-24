import os
import boto3
import requests
import json

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

INGEST_BUCKET = os.environ['INGEST_BUCKET']
RAW_DATA_PATH = os.environ['RAW_DATA_PATH']
LOCATION_QUERY_STRING = os.environ['LOCATION_QUERY_STRING']


def get_weather_data(query_string):
    logger.info('Get weather data from wttr.in')
    data_request = requests.get(f'https://wttr.in/{query_string}?format=j1')

    try:
        data_request.raise_for_status()
    except requests.exceptions.HTTPError:
        logger.exception('Failed to get weather data')
    else:
        logger.info('Successfully got weather data')

    return data_request


def save_to_s3(data, event_time):
    logger.info('Saving data to s3')
    s3 = boto3.client('s3')

    s3.put_object(
        Bucket=INGEST_BUCKET,
        Body=data,
        Key=f'{RAW_DATA_PATH}/{event_time}.json',
        ContentType='application/json',
    )

    logger.info('Data saved to s3')


def handler(event, context):

    weather_data = get_weather_data(LOCATION_QUERY_STRING.replace(' ', '+'))

    if weather_data.status_code == 200:
        save_to_s3(weather_data.content, event['time'])

        body = {
            'uploaded': 'true',
            'bucket': INGEST_BUCKET,
            'path': f'{RAW_DATA_PATH}/{event["time"]}.json'
        }

    else:
        body = {
            'uploaded': 'false',
            'error': weather_data.text
        }

    return {
        'statusCode': weather_data.status_code,
        'body': json.dumps(body)
    }
