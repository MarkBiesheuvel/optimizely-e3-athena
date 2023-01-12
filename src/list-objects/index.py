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


def handler(event, context):
    # TODO: input validation

    # Get settings from event
    token = event['token']

    s3_client, account_id = get_s3_client(token)
    sqs_client = boto3.client('sqs')

    # Create paginator as number of objects might exceed limit
    # TODO: narrow down by date range
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=SOURCE_BUCKET_NAME,
        Prefix='v1/account_id={}/'.format(account_id),
        PaginationConfig={
            'PageSize': 100,
        },
    )

    # Iterate over all pages in the paginator
    for response in response_iterator:

        # Get only the object key and discard the rest
        object_keys = [
            s3_object['Key'] for s3_object in response['Contents']
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


# Branch used for local development
if __name__ == '__main__':
    event = {
        'token': environ['OPTIMIZELY_API_TOKEN']
    }
    handler(event, None)
