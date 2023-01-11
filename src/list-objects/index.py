#!/usr/bin/env python3
import requests
import boto3
import re


def handler(event, context):
    # Get settings from event
    token = event['token']

    # Make request to Optimizely auth API
    response = requests.get(
        'https://api.optimizely.com/v2/export/credentials',
        headers={
            'Authorization': 'Bearer {}'.format(token)
        }
    ).json()

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

    # Create S3 client
    client = boto3.client('s3',
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
    )

    # Create paginator as number of objects might exceed limit
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket='optimizely-events-data',
        Prefix='v1/account_id={}/'.format(account_id),
    )

    # Send an SQS message for each page of 1000 objects
    for response in response_iterator:
        keys = [
            s3_object['Key'] for s3_object in response['Contents']
        ]

        # TODO: send keys as an SQS message
        print(len(keys))


# Branch used for local development
if __name__ == '__main__':
    from os import environ
    handler({'token': environ['OPTIMIZELY_API_TOKEN']}, None)
