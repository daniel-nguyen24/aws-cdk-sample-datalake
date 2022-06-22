import os
from webbrowser import get
import boto3
import requests
import json

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

INGEST_BUCKET = os.environ['INGEST_BUCKET']


def get_weather_data():
    logger.info('Get weather data from wttr.in')
    data_request = requests.get('https://wttr.in/Melbourne+VIC?format=j1')

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
        Key=f'weather-data/Melbourne/{event_time}.json',
        ContentType='application/json'
    )

    logger.info('Data saved to s3')


def handler(event, context):

    weather_data = get_weather_data()

    if weather_data.status_code == 200:
        save_to_s3(weather_data.content, event['time'])

        body = {
            'uploaded': 'true',
            'bucket': INGEST_BUCKET,
            'path': f'weather-data/Melbourne/{event["time"]}.json'
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
