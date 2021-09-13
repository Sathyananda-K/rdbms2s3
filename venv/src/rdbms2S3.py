import pyodbc
import boto3
from botocore.exceptions import NoCredentialsError
from boto3.s3.transfer import TransferConfig
import json
import os
import sys
import argparse
from datetime import datetime
import db2csv
import s3uploader
from s3uploader import MultiPartUploader

# Argument parsing
#------------------------------------------
parser = argparse.ArgumentParser(description='argument e.g: -tn "tableName" -sd "StartDate" -ed "StartDate"')
parser.add_argument('-tn', '--TableName', help='TableName for extract', required=True)
parser.add_argument('-sd', '--StartDate', help='e.g. "2021-12-12 12:12:12.000000" ', required=False)
parser.add_argument('-ed', '--EndDate', help='e.g. "2021-12-12 12:12:12.000000" ', required=False)
param = vars(parser.parse_args())

# Load config file to dict
with open('config.json', 'r') as config:
    configData = json.load(config)

# Load parameter file into dict
#------------------------------
# with open('localsqlparam.json', 'r') as config:
#     param = json.load(config)

# Load config data
#-----------------
DSN_Name = 'DSN=DBT1'
Source = configData["SOURCE"]
s3Bucket = configData["s3Bucket"]

# Prepare required tablename, filename and prefix
#------------------------------------------------
Tablename = param["TableName"]
filename = datetime.now().strftime("%Y-%m-%d_" + Tablename + ".csv")
prefix = datetime.now().strftime(Source+"/" + Tablename + "/%Y/%m/")

# Read SQL from sql.txt file from S3 bucket
#------------------------------------------
# s3 = boto3.resource('s3',
#                     aws_access_key_id=" ",
#                     aws_secret_access_key=" ",
#                     aws_session_token=" ")
# key = Source+'/'+Tablename+".txt"
# obj = s3.Object('s3_bucket_where_SQL_resides', key)
# sql = obj.get()['Body'].read().decode('utf-8')
# sql_prepared = sql.format(**param)

# Read SQL from sql.txt file from local
#--------------------------------------
f = open(Tablename + ".txt", "r")
sql = f.read()
sql_prepared = sql.format(**param)

# DB connectivity with utf-8 encoding
conn = pyodbc.connect(DSN_Name)
conn.setencoding(encoding='utf-8')
conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')

# Multipart Uploader where csv file is created directly on S3
#------------------------------------------------------------
output = MultiPartUploader(s3Bucket, prefix + filename)

# Extract headers and rows to CSV
#--------------------------------
extract = db2csv.query_to_csv(conn, sql_prepared, output)

uploadedTS = datetime.now().strftime("%Y%m%d_%H%M%S")
logfilename = Source+"-"+Tablename+"-"+uploadedTS+".json"

if extract.status:
    output.close()
    logop = {'Process Successful': 'True',
             'TableName': Tablename,
             'File Name': filename,
             'S3 bucket': s3Bucket,
             'Object Name': prefix + filename,
             'No of Rows Extract': extract.row_count,
             'EndDate': param["EndDate"],
             'Time Taken (in Seconds)': extract.time,
             'File Uploaded TimeStamp': uploadedTS}
    print('Process Successful')
    # Serializing json
    # ----------------
    json_object = json.dumps(logop, indent=4)
    with open(logfilename, "w") as outfile:
        outfile.write(json_object)
    sys.exit()
else:
    output.abort()
    logop = {'Process Successful': 'False',
             'Error Message': extract.error}
    print('Process failed')
    # Serializing json
    # ----------------
    json_object = json.dumps(logop, indent=4)
    with open(logfilename, "w") as outfile:
        outfile.write(json_object)
    sys.exit(1)
