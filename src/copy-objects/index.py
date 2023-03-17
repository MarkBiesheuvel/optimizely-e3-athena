#!/usr/bin/env python3
from urllib.request import Request, urlopen
from os import environ
import boto3
import re
import json

# S3 bucket names
SOURCE_BUCKET_NAME = 'optimizely-events-data'
DESTINATION_BUCKET_NAME = environ['DESTINATION_BUCKET_NAME']

# Overwrite the same file again and again
TEMPORARY_FILENAME = '/tmp/data'

# Regular expression to rewrite the S3 key to improve performance in Athena
ORIGINAL_KEY_REGEX = re.compile(
    r'v1/account_id=([0-9]+)/type=([a-z]+)/date=([0-9]+)-([0-9]+)-([0-9]+)/([a-z]+)=([a-zA-Z0-9_]+)/'
)
NEW_KEY_REPLACEMENT = r'\2/account=\1/\6=\7/'


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

    # Create S3 client using credentials from Optimizely
    return boto3.client('s3',
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
    )


def does_object_exist(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def handler(event, context):
    for record in event['Records']:
        message = json.loads(record['body'])

        token = message['token']
        object_keys = message['object_keys']

        source_s3_client = get_s3_client(token)
        destination_s3_client = boto3.client('s3')

        for source_key in object_keys:
            # Create a real counter, since some objects might be skipped
            counter = 0

            # Skip source keys that do not match the expected regex
            if not ORIGINAL_KEY_REGEX.match(source_key):
                continue

            # Create destination by performing a regex replace
            destination_key = ORIGINAL_KEY_REGEX.sub(
                NEW_KEY_REPLACEMENT,
                source_key
            )

            # Skip if `destination_key` already exists
            if does_object_exist(destination_s3_client, DESTINATION_BUCKET_NAME, destination_key):
                continue

            # Download from source bucket using source credentials
            source_s3_client.download_file(
                Filename=TEMPORARY_FILENAME,
                Bucket=SOURCE_BUCKET_NAME,
                Key=source_key,
            )

            # Upload to bucket in own account
            destination_s3_client.upload_file(
                Filename=TEMPORARY_FILENAME,
                Bucket=DESTINATION_BUCKET_NAME,
                Key=destination_key,
            )

            # Increment counter
            counter += 1

        print('Copied {} objects to this account'.format(counter))


# Branch used for local development
if __name__ == '__main__':
    event = {
        'Records': [
            {
                'body': json.dumps({
                    'token': environ['OPTIMIZELY_API_TOKEN'],
                    'object_keys': [
                        'v1/account_id=21537940595/type=events/date=2022-09-02/event=21514690867_button_1/part-00000-15a7b141-02da-4ccf-908f-3018698f4273.c000.snappy.parquet',
                    ]
                })
            }
        ]
    }
    handler(event, None)
