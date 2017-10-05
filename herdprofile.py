from __future__ import print_function
import boto3
import os
import sys
import uuid
import pandas as pd
import json
import io

s3 = boto3.client('s3')
     
def handler(event, context):
    for record in event['Records']:
        ### get the bucket name and file name that triggered the lambda
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'] 
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), key.replace('/','-'))
        s3.download_file(bucket, key.replace('+',' '), download_path)
        print ('downloaded herdprofile file ')
        herdProfile = pd.read_excel(download_path, skiprows=2, header=None, names=['id'])
        herdProfileList = herdProfile['id'].values.tolist()
        herdProfilejSonString = json.dumps(herdProfileList)
        s3.upload_fileobj(io.StringIO(herdProfilejSonString.decode('utf-8')), 'yewtu-gfl-dashboard','herdprofile/herdprofile.json')
        print ('all done')