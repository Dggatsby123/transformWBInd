import json
import pandas as pd
import os
import boto3

s3_bucketname = os.environ['s3_bucketname']	
s3_ocha_transformed_key	= os.environ['s3_ocha_wbtransformed_key'] 
s3_ocha_s3_folder = os.environ['s3_ocha_s3_folder'] 
temp_output_filename = os.environ['temp_output_filename']
temp_output_filepath = os.environ['temp_output_filepath']	
wb_feed_url = os.environ['wb_feed_url']

def transform_handler(event, context):
    transformIndicators()
    write_to_s3()

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('WB Indicators file successfully transformed')
    }

def write_to_s3( ):
   s3_client = boto3.client('s3')
   object = s3_client.upload_file(temp_output_filepath+s3_ocha_transformed_key,s3_bucketname,s3_ocha_s3_folder+s3_ocha_transformed_key)

def transformIndicators():
    dfSource = pd.read_excel(wb_feed_url, 'Data', skiprows=3).set_index(['Country Name','Country Code'])
    dfSource.drop(['Indicator Name','Indicator Code'], axis=1,inplace=True)
    dfTransposed = dfSource.T.unstack(level=1)
    dfTransposed.to_csv(temp_output_filepath+temp_output_filename)
    
    # FIX ME : Since df2 is a series, couldn't override column names, so had to load the dataframe again.
    dfFormatted = pd.read_csv(temp_output_filepath+temp_output_filename, encoding='utf-8')
    dfFormatted.columns =['Country Name', 'Country Code', 'Year', 'Population total']
    dfFormatted.to_csv(temp_output_filepath+s3_ocha_transformed_key, index=False)
    return
