#!/usr/bin/env python3
import requests
import boto3
import re


def handler(event, context):
    # TODO: iterate over S3 keys

    # Regular expression to rewrite the S3 key to improve performance in Athena
    # TODO: skip keys that are already imported
    original_key_regex = re.compile(
        r'v1/account_id=([0-9]+)/type=([a-z]+)/date=([0-9]+)-([0-9]+)-([0-9]+)/([a-z]+)=([a-zA-Z0-9_]+)/'
    )
    new_key_replacement = r'type=\2/account_id=\1/\6=\7/year=\3/month=\4/day=\5/'

    for response in response_iterator:
        for s3_object in response['Contents']:
            original_key = s3_object['Key']

            if original_key_regex.match(original_key):

                new_key = original_key_regex.sub(
                    new_key_replacement,
                    original_key
                )


# Branch used for local development
if __name__ == '__main__':
    handler({}, None)
