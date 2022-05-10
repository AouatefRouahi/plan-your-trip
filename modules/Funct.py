#!/usr/bin/env python
# coding: utf-8

## ********************************************* Libraries
# AWS SDK for AWS service
import boto3

# exceptions handling (https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html)
import botocore 


## ********************************************* csv files


def read_csv(path):
    '''read csv and return it as a list of dictionaries, one per row'''
    with open(path, 'r') as f:
        return list(DictReader(f))


def write_csv(data, path, mode='w'):
    '''write data to csv or append to existing one'''
    if mode not in 'wa':  # 'a' mode will append to the existing file, if it exists
        raise ValueError("mode should be either 'w' or 'a'")  
    
    with open(path, mode) as f:
        # data[0] because we get one data point per request (limit = 1)
        writer = DictWriter(f, fieldnames=data[0].keys())
        if mode == 'w':
            writer.writeheader() 
            # after this first insertion, we are no more in mode writing but appending for next insertions
            # so we go straight to the for loop

        for row in data:
            writer.writerow(row)  # mode =a implicitely 


## ********************************************* text files

def write_txt(data, path, mode ='w'):
    # data is any iterable without keys
    if mode not in 'wa':  # 'a' mode will append to the existing file, if it exists
        raise ValueError("mode should be either 'w' or 'a'")  
    
    with open(path, mode) as f:
        for row in data:
            f.write(row)
            f.write('\n')
    print('Content saved')


def read_txt(path, mode='r'):
    # Open file  
    try:
        fileHandler = open(path, mode)
    except FileNotFoundError:
        print('File does not exist')
    
    # Get list of all lines in file
    listOfLines = fileHandler.readlines()
    # Close file 
    fileHandler.close()
    # Iterate over the lines
    data = []
    for line in listOfLines:
        data.append(line.strip())
    
    print('Data retrieved')
    return data


## ********************************************* Data Lake Amazon S3

def create_bucket(bucket_name):
    # boto3 (AWS service S3)
    # open a boto3 s3 session and create a new bucket (data lake) where we will save all retrieved data  
    session = boto3.Session(aws_access_key_id="", 
                            aws_secret_access_key="")

    s3_client = session.client('s3')
    try:
        bucket = s3_client.create_bucket(Bucket=bucket_name)
        print('Bucket created')
    except s3.meta.client.exceptions.BucketAlreadyExists as e:
        print('Bucket already exists!')


# key = file name in the Data lake 
# file_name = file name in your local data store
def download_file_dl(bucket_name, key, file_name):
    session = boto3.Session(aws_access_key_id="", 
                            aws_secret_access_key="")

    s3_client = session.client('s3')
    try:
        s3_client.download_file(bucket_name, key, file_name)
        print('File downloaded')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print('The object does not exist !!')
        else:
            print ("Something Else !!",e)


def upload_file_dl(file_name, bucket_name, key):
    session = boto3.Session(aws_access_key_id="", 
                            aws_secret_access_key="")

    s3_client = session.client('s3')
    # Upload the file to the bucket
    try:
        response = s3_client.upload_file(file_name, bucket_name, key)
        print('File uploaded')
    except ClientError as e:
        print('Unexpected Error !!', e)

