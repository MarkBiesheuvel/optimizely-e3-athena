#!/usr/bin/env python3
from urllib.request import Request, urlopen
from os import environ
from datetime import date, timedelta
import boto3
import re
import json

SOURCE_BUCKET_NAME = 'optimizely-events-data'
QUEUE_URL = environ['QUEUE_URL']


# Source: https://stackoverflow.com/a/1060330/825547
def daterange(start_date, end_date):
    delta = timedelta(days=1)
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += delta


# TODO: move function to Lambda layer
def get_s3_client(token):
    # Make request to Optimizely auth API
    request = Request(
        'https://api.optimizely.com/v2/export/credentials?duration=1h',
        headers={
            'Authorization': 'Bearer {}'.format(token)
        }
    )
    response = json.loads(urlopen(request).read().decode())

    # Unpack data from response
    credentials = response['credentials']
    key_id = credentials['accessKeyId']
    secret_key = credentials['secretAccessKey']
    session_token = credentials['sessionToken']

    account_id = re.sub(
        r'.*/account_id=([0-9]+)/',
        r'\1',
        response['s3Path']
    )

    # Create S3 client using credentials from Optimizely
    s3_client = boto3.client('s3',
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
    )

    return (s3_client, account_id)


def list_objects(s3_client, sqs_client, token, prefix):
    # Create paginator as number of objects might exceed limit
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=SOURCE_BUCKET_NAME,
        Prefix=prefix,
    )

    # Iterate over all pages in the paginator
    # Get only the object key and discard the rest
    return (
        s3_object['Key']
        for response in response_iterator
        if response['KeyCount'] > 0
        for s3_object in response['Contents']
        if not s3_object['Key'].endswith('_SUCCESS')
    )


def make_prefix(account_id, table, year, month, day):
    return 'v1/account_id={}/type={}/date={:04d}-{:02d}-{:02d}'.format(
        account_id,
        table,
        year,
        month,
        day,
    )

def handler(event, context):
    # Get settings from event
    token = event['token']
    start = event['start']
    end = event['end']

    start_date = date(start['year'], start['month'], start['day'])
    end_date = date(start['year'], end['month'], end['day'])

    # AWS SDK clients
    s3_client, account_id = get_s3_client(token)
    sqs_client = boto3.client('sqs')

    # Iterate over both decisions and events
    prefixes = (
        make_prefix(account_id, table, current_date.year, current_date.month, current_date.day)
        for current_date in daterange(start_date, end_date)
        for table in ['decisions', 'events']
    )

    object_keys = list(
        object_key
        for prefix in prefixes
        for object_key in list_objects(s3_client, sqs_client, token, prefix)
    )

    # Send messages of 200 object keys
    n = 200

    # Iterate over batches
    for i in range(0, len(object_keys), n):
        batch = object_keys[i:i + n]

        # Send list of object keys as message to SQS
        sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps({
                'token': token,
                'object_keys': batch,
            })
        )

        print('Sent message to SQS containing {} object keys'.format(len(batch)))


# DO NOT CHANGE THIS IN THE LAMBDA FUNCTION
# THIS IS FOR LOCAL DEVELOPMENT ONLY
if __name__ == '__main__':
    event = {
        'token': environ['OPTIMIZELY_API_TOKEN'],
        'start': {
            'year': 2023,
            'month': 1,
            'day': 1,
        },
        'end': {
            'year': 2023,
            'month': 3,
            'day': 16,
        },
    }
    handler(event, None)
