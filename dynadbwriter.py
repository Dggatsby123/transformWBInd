import boto3
import os
import csv
import codecs
import sys

dynamodb = boto3.resource('dynamodb')
tableName = os.environ['table']
s3_bucketname = os.environ['s3_bucketname']
s3_ocha_s3_folder = os.environ['s3_ocha_s3_folder'] 
s3_ocha_transformed_key	= os.environ['s3_ocha_transformed_key'] 

def dynamodb_handler(event, context):
   s3 = boto3.resource('s3')
   obj = s3.Object(s3_bucketname,s3_ocha_s3_folder+s3_ocha_transformed_key).get()['Body']
   table = dynamodb.Table(tableName)
   batch_size = 100
   batch = []

   #DictReader is a generator; not stored in memory
   for row in csv.DictReader(codecs.getreader('utf-8')(obj)):
      if len(batch) >= batch_size:
         write_to_dynamo(batch)
         batch.clear()
      print('appending row: '+str(row))     
      batch.append(row)
   if batch:
      write_to_dynamo(batch)
   return


def write_to_dynamo(rows):
    try:
      table = dynamodb.Table(tableName)
    except Exception as e:
      print("Error loading DynamoDB table. Check if table was created correctly and environment variable."+str(e))
      raise e 

    print("inserting batch items: "+str(len(rows)))
    with table.batch_writer() as batch:
        for i in range(len(rows)):
            batch.put_item(
               Item=rows[i]
            )
  