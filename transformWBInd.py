import json
import pandas as pd
import os
import boto3
import csv
import sys
import traceback
import dynadbwriter as dw

s3_bucketname = os.environ['s3_bucketname']	
s3_ocha_s3_folder = os.environ['s3_ocha_s3_folder'] 
s3_ocha_transformed_key	= os.environ['s3_ocha_transformed_key'] 
temp_output_filename = os.environ['temp_output_filename']
temp_output_filepath = os.environ['temp_output_filepath']	
wb_feed_url = os.environ['wb_feed_url']

def transform_handler(event, context):
    
    try:
        transformWBIndicators()
        write_to_s3()
        dw.dynamodb_handler(event, context)
    except Exception as ex:
        traceback.print_exception(type(ex), ex, ex.__traceback__)
        return {
        'statusCode': 500,
        'body': json.dumps('Error reading/transforming WB indicators file', str(ex))
        }
    
    # If everything executes with our exception, return 200
    return {
        'statusCode': 200,
        'body': json.dumps('WB Indicators file successfully transformed and stored')
    }

def write_to_s3( ):
   s3_client = boto3.client('s3')
   object = s3_client.upload_file(temp_output_filepath+s3_ocha_transformed_key,s3_bucketname,s3_ocha_s3_folder+s3_ocha_transformed_key)

def transformWBIndicators():
    # This method reads the excel file from Worldbank site, transform into desired format inlcuding column names
    dfSource = pd.read_excel(wb_feed_url, 'Data', skiprows=3).set_index(['Country Name','Country Code'])
    dfSource.drop(['Indicator Name','Indicator Code'], axis=1,inplace=True)
    dfTransposed = dfSource.T.unstack(level=1)
    dfTransposed.to_csv(temp_output_filepath+temp_output_filename)
    
    # FIX ME : Since df2 is a series, couldn't override column names, so had to load the dataframe again.
    dfFormatted = pd.read_csv(temp_output_filepath+temp_output_filename, encoding='utf-8')
    dfFormatted.columns =['Country Name', 'Country Code', 'Year', 'Population total']
    dfFormatted.index = range(len(dfFormatted))
    dfFormatted.index.name='row_num'
    dfFormatted.to_csv(temp_output_filepath+s3_ocha_transformed_key, index=True)
    return

  