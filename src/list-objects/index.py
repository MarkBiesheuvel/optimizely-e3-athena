#!/usr/bin/env python3
from urllib.request import Request, urlopen
from os import environ
import boto3
import re
import json

SOURCE_BUCKET_NAME = 'optimizely-events-data'
QUEUE_URL = environ['QUEUE_URL']


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
        PaginationConfig={
            'PageSize': 200,
        },
    )

    # Iterate over all pages in the paginator
    for response in response_iterator:

        if response['KeyCount'] == 0:
            print('No objects found for given token and date range')
            continue

        # Get only the object key and discard the rest
        object_keys = [
            s3_object['Key']
            for s3_object in response['Contents']
            if not s3_object['Key'].endswith('_SUCCESS')
        ]

        # Send list of object keys as message to SQS
        sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps({
                'token': token,
                'object_keys': object_keys,
            })
        )

        print('Sent message to SQS containing {} object keys'.format(len(object_keys)))


def make_prefix(account_id, type_, year=None, month=None, day=None):
    prefix = 'v1/account_id={}/type={}'.format(account_id, type_)

    if year is not None:
        prefix = '{}/date={:04d}'.format(prefix, year)

        if month is not None:
            prefix = '{}-{:02d}'.format(prefix, month)

            if day is not None:
                prefix = '{}-{:02d}'.format(prefix, day)

    return prefix


def handler(event, context):
    # Get settings from event
    token = event.get('token')
    year = event.get('year')
    month = event.get('month')
    day = event.get('day')

    # Input validation
    if token is None:
        raise 'No token provided.'
    if year is not None and not isinstance(year, int):
        raise 'Year needs to be either an integer or null.'
    if month is not None and not isinstance(year, int):
        raise 'Month needs to be either an integer or null.'
    if day is not None and not isinstance(year, int):
        raise 'Day needs to be either an integer or null.'

    # AWS SDK clients
    s3_client, account_id = get_s3_client(token)
    sqs_client = boto3.client('sqs')

    # Iterate over both decisions and events
    prefixes = [
        make_prefix(account_id, 'decisions', year, month, day),
        make_prefix(account_id, 'events', year, month, day),
    ]
    for prefix in prefixes:
        list_objects(s3_client, sqs_client, token, prefix)


# If-branch used for local development
if __name__ == '__main__':
    event = {
        'token': environ['OPTIMIZELY_API_TOKEN'],
        'year': 2023,
        'month': 1,
        'day': None,
    }
    handler(event, None)
