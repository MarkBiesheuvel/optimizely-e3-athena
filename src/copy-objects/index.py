#!/usr/bin/env python3
from urllib.request import Request, urlopen
from os import environ
import boto3
import re
import json

# 5MB, adjust as needed
PART_SIZE = 5 * 1024 * 1024

# S3 bucket names
SOURCE_BUCKET_NAME = 'optimizely-events-data'
DESTINATION_BUCKET_NAME = environ['DESTINATION_BUCKET_NAME']

# Overwrite the same file again and again
TEMPORARY_FILENAME = '/tmp/data'

# Regular expression to rewrite the S3 key to improve performance in Athena
ORIGINAL_KEY_REGEX = re.compile(
    r'v1/account_id=([0-9]+)/type=([a-z]+)/date=([0-9]+)-([0-9]+)-([0-9]+)/([a-z]+)=(.+?)/'
)
NEW_KEY_REPLACEMENT = r'\2/account=\1/\6=\7/'


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


def get_file_size(client, bucket, key):
    response = client.head_object(Bucket=bucket, Key=key)
    return response['ContentLength']


def create_multipart_upload(client, bucket, key):
    response = client.create_multipart_upload(Bucket=bucket, Key=key)
    return response['UploadId']


def download_part(client, bucket, key, offset_from, offset_to):
    response = client.get_object(
        Bucket=bucket,
        Key=key,
        Range='bytes={0}-{1}'.format(
            offset_from,
            offset_to
        )
    )

    return response['Body'].read()


def upload_body(client, bucket, key, body):
    response = response = client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body
    )

    return response['ETag']


def upload_part(client, bucket, key, upload_id, part_number, body):
    response = client.upload_part(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        PartNumber=part_number,
        Body=body
    )

    return response['ETag']


def complete_multipart_upload(client, bucket, key, upload_id, parts):
    client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts}
    )


def stream_file(source_s3_client, destination_s3_client, source_bucket, destination_bucket, source_key, destination_key):
    # Initialize counters
    parts = []
    part_number = 1
    offset = 0

     # Get file size
    file_size = get_file_size(source_s3_client, source_bucket, source_key)

    # Only perform multipart uploads on large files
    is_multipart_upload = file_size >= PART_SIZE

    # Initiate multipart upload
    if is_multipart_upload:
        upload_id = create_multipart_upload(destination_s3_client, destination_bucket, destination_key)

    # Copy part by part
    while offset < file_size:
        # Calculate the end offset
        offset_end = min(offset + PART_SIZE, file_size) - 1

        # Download part
        part_body = download_part(source_s3_client, source_bucket, source_key, offset, offset_end)

        if is_multipart_upload:
            # Upload part
            etag = upload_part(destination_s3_client, destination_bucket, destination_key, upload_id, part_number, part_body)

            # Prepare complete_multipart_upload request
            parts.append({
                'ETag': etag,
                'PartNumber': part_number
            })
        else:
            upload_body(destination_s3_client, destination_bucket, destination_key, part_body)

        # Increase counters for next iteration
        offset += PART_SIZE
        part_number += 1

    # Complete multipart upload
    if is_multipart_upload:
        complete_multipart_upload(destination_s3_client, destination_bucket, destination_key, upload_id, parts)

    print("Completed upload of '{}' in {} parts".format(destination_key, len(parts)))


def handler(event, context):
    for record in event['Records']:
        message = json.loads(record['body'])

        token = message['token']
        object_keys = message['object_keys']

        source_s3_client = get_s3_client(token)
        destination_s3_client = boto3.client('s3')

        # Create a real counter, since some objects might be skipped
        counter = 0

        for source_key in object_keys:
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
            stream_file(
                source_s3_client,
                destination_s3_client,
                SOURCE_BUCKET_NAME,
                DESTINATION_BUCKET_NAME,
                source_key,
                destination_key
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
                        'v1/account_id=21537940595/type=events/date=2024-01-01/event=NULL/part-00000-b5f3005a-d480-4d53-a600-0e9824e0fb60.c000.snappy.parquet',
                    ]
                })
            }
        ]
    }
    handler(event, None)
