#!/usr/bin/env python3

import logging
import boto3
from botocore.exceptions import ClientError
import requests
import argparse
from botocore.client import Config

parser = argparse.ArgumentParser(description="Generate expiring S3 URL")
parser.add_argument("-b", "--bucket", help = "S3 bucket name", required=True, type=str)
parser.add_argument("-o", "--object", help = "S3 object name", required=True, type=str)
parser.add_argument("-e", "--expiry", help = "Expiry time in seconds", required=True, type=int)
args = parser.parse_args()

def create_presigned_url(bucket_name, object_name, expiration=args.expiry):
	payload = { "use_ssl": True, "verify": True }

	# Generate a presigned URL for the S3 object
	s3_client = boto3.client( 's3', payload, config=Config(signature_version='s3v4', region_name=None))
	try:
		response = s3_client.generate_presigned_url('get_object',
													Params={'Bucket': bucket_name,
															'Key': object_name},
													ExpiresIn=expiration)
	except ClientError as e:
		logging.error(e)
		return None

	# The response contains the presigned URL
	return response

url = create_presigned_url(args.bucket, args.object, args.expiry)
if url is not None:
	response = requests.get(url)
	print(url)
