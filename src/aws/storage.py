import os
import boto3
import botocore
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


class S3:
    def __init__(self):
        self.session = boto3.Session(
            aws_access_key_id=os.getenv('aws_access_key_id'),
            aws_secret_access_key=os.getenv('aws_secret_access_key'),
            region_name=os.getenv('region_name'),
        )
        self.client = boto3.client('s3', aws_access_key_id=os.getenv('aws_access_key_id'), 
            aws_secret_access_key=os.getenv('aws_secret_access_key'), 
            region_name=os.getenv('region_name'))

    def upload_file(self, bucket=None):
        pass

    def list_files(self, bucket=None, prefix = None):
        client = self.client
        result = client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
        file_name_array = []
        
        if result.get('Contents') is None:
            return None
        else:
            for object in result.get('Contents'):
                # print (object['Key'])
                file_name_array.append(object['Key'])
        return file_name_array

    def download_file(self, bucket=None, file=None, destination=None):
        if not os.path.exists('storage'):
            os.makedirs('storage')

        if destination is None:
            destination = str(Path('storage/' + os.path.basename(file)))

        try:
            self.session.resource('s3').Bucket(bucket).download_file(file, destination)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print('The file does not exist.')
                return 404
            else:
                raise
