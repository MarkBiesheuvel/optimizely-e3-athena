#!/usr/bin/env python3
from urllib.request import Request, urlopen
from os import environ
import boto3
import re
import json

# Regular expression to rewrite the S3 key to improve performance in Athena

ORIGINAL_KEY_REGEX = re.compile(
    r'v1/account_id=([0-9]+)/type=([a-z]+)/date=([0-9]+)-([0-9]+)-([0-9]+)/([a-z]+)=([a-zA-Z0-9_]+)/'
)
NEW_KEY_REPLACEMENT = r'type=\2/account_id=\1/\6=\7/year=\3/month=\4/day=\5/'


def handler(event, context):
    for record in event['Records']:
        message = json.loads(record['body'])

        token = message['token']
        object_keys = message['object_keys']

        for original_key in object_keys:
            if ORIGINAL_KEY_REGEX.match(original_key):

                new_key = ORIGINAL_KEY_REGEX.sub(
                    NEW_KEY_REPLACEMENT,
                    original_key
                )

                # TODO: skip new keys that are already imported
                print(new_key)


# Branch used for local development
if __name__ == '__main__':
    handler({}, None)
