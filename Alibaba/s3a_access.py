#!/usr/bin/python3

#usage: test_boto.py put bucket_name object_name
# example: python run_boto.py put traces alibaba_clusterdata_v2018/machine_usage.csv 
# example run_boto.py get traces  alibaba_clusterdata_v2018/batch_instance.csv

import boto3
from botocore.client import Config
import sys

#Kaizenold
#access_key = "5b0d738e101946e2acee17e46e04fd26"
#secret_key = "c5feba0ca6d54cadab3e80a98d849f5d"
#endpoint_url="https://swift-kaizen.massopen.cloud"

#Kaizen
access_key = "2551ad009800493193855ada8bea85e9"
secret_key = "5d394639ecc744dca55360601f23089d"
endpoint_url="https://kzn-swift.massopen.cloud"

is_secure = False

client = boto3.client(service_name='s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                    endpoint_url=endpoint_url)

bucket_name = sys.argv[2]
obj_name = sys.argv[3]
file_name = sys.argv[4]

if sys.argv[1] == "put":
    data = open(obj_name, 'rb')
    client.create_bucket(Bucket=bucket_name)
    check = client.put_object(Key=obj_name, Body=data, Bucket=bucket_name)
elif sys.argv[1] == "get":
    print(bucket_name + ", " + obj_name + ", " + file_name)
    client.download_file(bucket_name, obj_name, file_name)
