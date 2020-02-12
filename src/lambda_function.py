import json
import psycopg2
import psycopg2.extras
import os
import sys
import urllib

IAM_ROLE = os.environ['IAM_ROLE']
TABLE_NAME = os.environ['TABLE_NAME']
REDSHIFT_DATABASE = os.environ['REDSHIFT_DATABASE']
REDSHIFT_USER = os.environ['REDSHIFT_USER']
REDSHIFT_PASSWD = os.environ['REDSHIFT_PASSWD']
REDSHIFT_PORT = os.environ['REDSHIFT_PORT']
REDSHIFT_ENDPOINT = os.environ['REDSHIFT_ENDPOINT']
REDSHIFT_QUERY1 = "drop table if exists {{TABLE_NAME}};"
REDSHIFT_QUERY2 = """
create table if not exists {{TABLE_NAME}} (
  rowid int4 not null,
  orderid varchar(20) null,
  orderdate date null,
  shipdate date null,
  shipmode varchar(20) null,
  customerid varchar(20) null,
  customername varchar(50) null,
  segment varchar(20) null,
  country varchar(50) null,
  city varchar(50) null,
  state varchar(50) null,
  postalcode int8 null,
  region varchar(50) null,
  productid varchar(50) null,
  category varchar(50) null,
  subcategory varchar(50) null,
  productname varchar(256) null,
  sales numeric(8,2) null,
  quantity int4 null,
  discount numeric(8,2) null,
  profit numeric(10,4) null
)
distkey(customerid)
"""
REDSHIFT_QUERY3 = """
  copy {{TABLE_NAME}}
  from 's3://{{BUCKET}}/{{KEY}}'
  iam_role '{{IAM_ROLE}}'
  delimiter ';'
  IGNOREHEADER 1
  DATEFORMAT 'YYYYMMDD';
"""
def getParseS3Message(message):
  #s3Event = json.loads(event)

  s3 = message["Records"][0]["s3"]

  #print(s3)

  bucket = s3["bucket"]["name"]
  key = urllib.parse.unquote_plus(s3["object"]["key"])

  print(f'The Lambda has been triggered by the bucket {bucket}.\nA file {key} is going to be ingested')

  return (bucket, key)

def getRedshiftConnection():
  try:
    conn = psycopg2.connect(
      dbname=REDSHIFT_DATABASE,
      user=REDSHIFT_USER,
      password=REDSHIFT_PASSWD,
      port=REDSHIFT_PORT,
      host=REDSHIFT_ENDPOINT)
    conn.autocommit = True

  except Exception as ERROR:
    print("Connection Issue: ")
    print(ERROR)
    sys.exit(1)

  return conn

def getS3CopyQuery(bucket, key):
  return REDSHIFT_QUERY3\
    .replace('{{TABLE_NAME}}', TABLE_NAME)\
    .replace('{{BUCKET}}', bucket)\
    .replace('{{KEY}}', key)\
    .replace('{{IAM_ROLE}}', IAM_ROLE)\

def runCopyQuery(conn, copyQuery):
  try:
    cursor = conn.cursor()
    #print("Cursor: ")
    cursor.execute(REDSHIFT_QUERY1.replace('{{TABLE_NAME}}', TABLE_NAME))
    cursor.execute(REDSHIFT_QUERY2.replace('{{TABLE_NAME}}', TABLE_NAME))
    cursor.execute(copyQuery)
    cursor.close()
    conn.commit()
    conn.close()

  except Exception as ERROR:
    print("Execution Issue: ")
    print(ERROR)
    sys.exit(1)

def lambda_handler(event, context):
  # TODO implement

  (bucket, key) = getParseS3Message(event)

  conn = getRedshiftConnection()

  copyQuery = getS3CopyQuery(bucket, key)

  runCopyQuery(conn, copyQuery)

  #print(copy_query)

  print(f'the bucket name is: {bucket}/{key} has been loaded into {TABLE_NAME}')

  return f'the bucket name is: {bucket}/{key} has been loaded into {TABLE_NAME}'
