import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    # GitHub API URL for the specific release assets
    release_url = "https://api.github.com/repos/DataTalksClub/nyc-tlc-data/releases/tags/green"
    
    #data field definition of the data
    taxi_dtypes = {
                'VendorID': pd.Int64Dtype(),
                'passenger_count': pd.Int64Dtype(),
                'trip_distance': float,
                'RatecodeID':pd.Int64Dtype(),
                'store_and_fwd_flag':str,
                'PULocationID':pd.Int64Dtype(),
                'DOLocationID':pd.Int64Dtype(),
                'payment_type': pd.Int64Dtype(),
                'fare_amount': float,
                'extra':float,
                'mta_tax':float,
                'tip_amount':float,
                'tolls_amount':float,
                'improvement_surcharge':float,
                'total_amount':float,
                'congestion_surcharge':float
            }
    # native date parsing 
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    
    # Make a GET request to the GitHub API to fetch release information
    response = requests.get(release_url)
    response.raise_for_status()  # This will raise an error if the request failed

    # Extract the assets (files) from the release data
    assets = response.json().get('assets', [])

    # List all files
    download_urls=[]
    for asset in assets:
        
        file_name = asset['name'] 
        download_url = asset['browser_download_url']

        #get the year month portion of the file
        file_yearm = file_name.split('_')[-1].split('.', 1)[0]
        file_year  = int(file_yearm.split('-')[0])
        file_month = int(file_yearm.split('-')[-1])

        if((file_year == 2020) and (10 <= file_month <= 12)):
            # print(f"{file_name}  --  {file_yearm} -- {file_year} -- {file_month}")
            # print(download_url)
            download_urls.append(download_url)
    
    # Create an empty DataFrame list to append to 
    dframes = []
    for url in download_urls:
        df = pd.read_csv(url \
                        , sep=',' \
                        , compression='gzip' \
                        , dtype=taxi_dtypes \
                        , parse_dates=parse_dates)
        df['file_name'] = url.split('/')[-1]
        dframes.append(df)
    
    #merge all the dataframes in the list into 1 massive dataframe
    final_df = pd.concat(dframes)

    return final_df


@test
def test_output(output, *args) -> None:
    assert output[''] is not None, 'The output is undefined'
