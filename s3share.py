#!/usr/bin/env python3

import logging
import boto3
from botocore.exceptions import ClientError
import requests
import argparse
from botocore.client import Config

region = None

parser = argparse.ArgumentParser(description="Generate expiring S3 URL")
parser.add_argument("-b", "--bucket", help = "S3 bucket name", required=True, type=str)
parser.add_argument("-o", "--object", help = "S3 object name", required=True, type=str)
parser.add_argument("-r", "--region", help = "S3 region name", required=False, default='eu-west-1', type=str)
parser.add_argument("-e", "--expiry", help = "Expiry time in seconds", required=True, type=int)
args = parser.parse_args()

def create_presigned_url(bucket_name, object_name, expiration=args.expiry):
  client_config = Config(
    signature_version = 'v4',
    retries = {
      'max_attempts': 10,
      'mode': 'standard'
    }
  )

  s3_client = boto3.session.Session(
    region_name=boto3.client('s3').get_bucket_location(Bucket=bucket_name)["LocationConstraint"]
  ).client("s3", config=client_config)
  
  try:
    response = s3_client.generate_presigned_url('get_object', Params={
      'Bucket': bucket_name,
      'Key': object_name
    },
    ExpiresIn=expiration
  )
  except ClientError as e:
    logging.error(e)
    return None

  return response

url = create_presigned_url(args.bucket, args.object, args.expiry)
if url is not None:
  print(url)
