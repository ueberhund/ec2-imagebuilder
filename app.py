from arcgis.gis import GIS
import os
import datetime
import pandas as pd 
import boto3
import json
import time

def main():
    
    #Output file location
    output_location = './temp' 
    
    #Environment variables used in this code
    region = os.environ["REGION"]
    secret_name = os.environ["SECRET_NAME"]
    s3_bucket = os.environ["S3_BUCKET_NAME"]
    s3_key = os.environ["S3_KEY_NAME"]
    ss3_key_id = os.environ["SSE_KEY_ID"]
    
    #Pull username/password from Secrets Manager
    client = boto3.client('secretsmanager', region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    secret = response['SecretString']
    secret_dict = json.loads(secret)
    USERNAME = secret_dict['username']
    PASS = secret_dict['password']
    
    #Create the temp directory
    if not os.path.exists(output_location):
        os.makedirs(output_location)

    should_continue = False
    num_tries = 0

    #Try this a few times to make sure we get data
    while not should_continue and num_tries < 5:
        try: 
            num_tries += 1
            file_path = download_data(USERNAME, PASS, output_location)
            should_continue = True
        except Exception as e:
            print("Error downloading data: :{}".format(e))
            time.sleep(60)

    print("File path: {}".format(file_path))

    #Now process the data and delete all but the most current data collected
    df = pd.read_csv(file_path)

    #Pull out just the most recent data and save to CSV
    df['coll_start_date'] =  pd.to_datetime(df['coll_start_date']).dt.date
    df.sort_values(["coll_start_date", "adm1_name"], ascending=False)
    df.drop_duplicates(subset=["adm1_name"], keep="first", inplace=True)
    df.to_csv('./temp/food_security.csv', header=True)

    #Save the results to S3 if we have more than 150 rows
    if len(df.index) > 150:
        s3 = boto3.client('s3')
        s3.upload_file('./temp/food_security.csv', s3_bucket, s3_key, 
                    ExtraArgs={'ServerSideEncryption': 'aws:kms', "SSEKMSKeyId": ss3_key_id })
        print("Data saved to S3 with {} rows".format(len(df.index)))

def download_data(username, password, output_location):
    # Initialize the where clause
    where_clause = "1=1"  

    file_location = ""
    gis = GIS('https://hqfao-hub.maps.arcgis.com/home/group.html?id=4eca32aadf664a8f9a59d2dd68b7444d&view=list#content', username, password)
    
    # Get the group using the Group ID (From the group URL)
    group = gis.groups.get("4eca32aadf664a8f9a59d2dd68b7444d")
    group_items = group.content()
    for group_item in group_items:
        if group_item.title == "DIEM aggregated data (food security thematic area)":
            print(f"Processing the {group_item.title} dataset.")
            subset_feature_layer = group_item.layers[0]
            df = subset_feature_layer.query(where=where_clause, out_fields = "*", returnGeometry=False, as_df=True)
            df.to_csv(os.path.join(output_location,group_item.title + ".csv"))
            file_location = os.path.join(output_location, group_item.title + ".csv")
            print(f"Saved the {group_item.title} data in {output_location} .\n")
    print("Data downloaded")

    return file_location


if __name__ == "__main__":
    main()
