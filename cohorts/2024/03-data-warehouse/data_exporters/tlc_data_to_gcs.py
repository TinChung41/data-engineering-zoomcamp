from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import pandas as pd
from os import path
import requests
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

#google config
config_path = path.join(get_repo_path(), 'io_config.yaml')
config_profile = 'default'

gcp_config = ConfigFileLoader(config_path, config_profile)
my_storage = GoogleCloudStorage.with_config(gcp_config)
    

def check_file_exists(bucket,file_name):
    # ... other tasks ...
    if my_storage.exists(bucket, file_name):
        print(f"File exists!: {bucket}/{file_name}")
        is_exists = True
    else:
        is_exists = False
    
    return is_exists

def parquets_to_gcs(year, colour):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    bucket_name = 'de-homework-3'

    for x in range(1, 13):
        #construct url with colour / year / month 
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{colour}_tripdata_{year}-{x:02}.parquet"
                # https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-01.parquet
        filename    = url.split('/')[-1]
        temp_file   = f"/tmp/{filename}"
        object_path = f"{colour}/{filename}"
        
        #check if the file exists in the bucket; if not upload 
        if not(check_file_exists(bucket_name,filename)):    
            # Download the file
            response = requests.get(url)
            response.raise_for_status()

            # Save file locally
            with open(temp_file, "wb") as f:
                f.write(response.content)
            
            #read file in using pyarrow
            parquet_file = pq.ParquetFile(temp_file)
            data=[]
            #split the file into chucks to read
            for batch in parquet_file.iter_batches(batch_size=100000):
                d1 = batch.to_pandas()
                
                #ensure fields in correct datatype
                d1['VendorID']              = pd.to_numeric(d1['VendorID'])
                d1['passenger_count']       = pd.to_numeric(d1['passenger_count'])
                d1['trip_distance']         = pd.to_numeric(d1['trip_distance'])
                d1['RatecodeID']            = pd.to_numeric(d1['RatecodeID'])
                d1['store_and_fwd_flag']    = d1['store_and_fwd_flag'].astype(str)
                d1['PULocationID']          = pd.to_numeric(d1['PULocationID'])
                d1['DOLocationID']          = pd.to_numeric(d1['DOLocationID'])
                d1['payment_type']          = pd.to_numeric(d1['payment_type'])
                d1['fare_amount']           = pd.to_numeric(d1['fare_amount'])
                d1['extra']                 = pd.to_numeric(d1['extra'])
                d1['mta_tax']               = pd.to_numeric(d1['mta_tax'])
                d1['tip_amount']            = pd.to_numeric(d1['tip_amount'])
                d1['tolls_amount']          = pd.to_numeric(d1['tolls_amount'])
                d1['improvement_surcharge'] = pd.to_numeric(d1['improvement_surcharge'])
                d1['total_amount']          = pd.to_numeric(d1['total_amount'])
                d1['congestion_surcharge']  = pd.to_numeric(d1['congestion_surcharge'])
                d1["ehail_fee"]             = pd.to_numeric(d1["ehail_fee"])
                
                data.append(d1)
            
            df = pd.concat(data)
            
            #upload the file to gcs
            # GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
            my_storage.export(
                    df,
                    bucket_name,
                    object_path,
            )
 

@data_exporter
def output():
    parquets_to_gcs(2022,'green')
    # parquets_to_gcs(2020,'green')
    