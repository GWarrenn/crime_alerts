import pandas as pd
import urllib2
import json
import geopy
from geopy.geocoders import Nominatim
from geopy import distance
import sqlalchemy
from sqlalchemy import *
import datetime
import boto3
import ConfigParser
import argparse

def crime_alerts_main(upload,send_message):
    '''
    Main function to pull, process and send notification for DC crime data
    Currently designed to pull and process crime data for all users in crime_alerts_users table
    
    Two optional parameters:
    upload: Default value is true, if other specified will not upload new crime incidents to database
    send_message: Default value is true, if other specified will not send out text notifications to users
    
    '''
    
    select_statement = "select * from crime_alerts_users"
    
    engine = sqlalchemy.create_engine('postgresql://postgres@localhost:5432/postgres')
    conn = engine.connect()
    
    select_user_data = pd.read_sql(select_statement, conn) 
    
    for index,row in select_user_data.iterrows():
        user_name = row['user_name']
        user_phone_number = row['phone_number']
        crime_user_id = row['crime_user_id']
        
        print "Processing crime data for " + user_name.strip() + " at " + str(user_phone_number).strip()
        
        recent_crime = get_recent_crime_data(user_name)
        trim_cols =['attributes.CCN','attributes.BLOCK','attributes.BLOCK_GROUP','attributes.CENSUS_TRACT',
                    'geometry.y','geometry.x','date_start','attributes.OFFENSE','attributes.NEIGHBORHOOD_CLUSTER','date_reported']
        
        recent_crime = recent_crime[trim_cols]
        select_statement = "select * from crime_alerts_incidents where crime_user_id ="+str(crime_user_id)
        
        prev_week_db = pd.read_sql(select_statement, conn) 

        prev_week_db['crime_ccn_id'] = prev_week_db['crime_ccn_id'].astype('int')
        recent_crime['attributes.CCN'] = recent_crime['attributes.CCN'].astype('int')

        new_incidents = pd.merge(recent_crime,prev_week_db,
                                left_on = ['attributes.CCN'],
                                right_on = ['crime_ccn_id'],
                                how ='left')
        new_incidents = new_incidents[pd.isnull(new_incidents['crime_ccn_id'])]
        new_incidents = new_incidents[trim_cols]
        
        new_incidents = new_incidents.rename(index=str, columns={"attributes.CCN": "crime_ccn_id","attributes.BLOCK": "crime_address", "attributes.OFFENSE": "crime_type",
            "date_start": "crime_date_start","attributes.BLOCK_GROUP": "crime_block_group" ,"attributes.CENSUS_TRACT":"crime_census_tract",
            "geometry.y":"crime_latitude","geometry.x":"crime_longitude","attributes.NEIGHBORHOOD_CLUSTER":"crime_neighborhood","date_uploaded":"date_uploaded",
            'date_reported':'crime_date_reported'})
        
        new_cols = prev_week_db.columns.tolist()
        new_incidents['date_uploaded'] = pd.Timestamp.today()
        new_incidents['date_uploaded'] = new_incidents['date_uploaded'].astype('string')
        
        new_incidents['crime_user_id'] = crime_user_id
        
        new_incidents = new_incidents[new_cols]
        
        if new_incidents.empty == False :
            if upload:
                try:
                    print "Uploading "+ str(len(new_incidents.index)) + " new crime incidents to database"
                    new_incidents.to_sql('crime_alerts_incidents', conn,if_exists='append',index=False)
                except Exception as ex:
                    print "Error:"+ str(ex)
            if send_message:
                print "Sending "+ str(len(new_incidents.index)) + " new crime notifications to " + user_name
                for index,row in new_incidents.iterrows():
                    message = "Crime Alert: "+row['crime_type']+ " at "+ row['crime_address'] + " on " + str(row['crime_date_start'])
                    send_alert(user_phone_number,message)
        else:
            print "No new crime incidents to upload"
            
def get_recent_crime_data(user_name):
    '''
    Pull crime data from dc.gov API filtered within week and within half mile radius of user location
    
    '''

    print "Pulling last month of reported crime data from dc.gov" 
    last_ten_days = datetime.datetime.now() - datetime.timedelta(days=10)
    last_ten_days = last_ten_days.strftime('%Y-%m-%d')
    
    f = urllib2.urlopen('https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/0/query?where=REPORT_DAT%3Edate%20%27'+last_ten_days+'%27&outFields=*&outSR=4326&f=json')
    
    data = json.load(f)
    
    crime_df = pd.io.json.json_normalize(data['features'])
    
    crime_df['date_start'] = pd.to_datetime(crime_df['attributes.START_DATE'],unit='ms')
    crime_df['date_reported'] = pd.to_datetime(crime_df['attributes.REPORT_DAT'],unit='ms')

    crime_df['today'] = pd.to_datetime('today')
    crime_df['diff'] = crime_df['today']-crime_df['date_start']
    crime_df['diff_days'] = crime_df['diff'].dt.total_seconds() / (24 * 60 * 60)
    
    crime_df = crime_df.loc[crime_df['diff_days'] < 7]
    
    user_data = get_user_location(user_name)
    
    user_location = (user_data['user_latitude'].iloc[0],user_data['user_longitude'].iloc[0])
    
    for index, row in crime_df.iterrows():
        crime_location = (row['geometry.y'],row['geometry.x'])
        crime_df.loc[index, 'distance'] = distance.great_circle(crime_location, user_location).miles
    
    close_crimes = crime_df.loc[crime_df['distance'] < .5]
    
    return close_crimes

def get_user_location(user_name):
    '''
    Pull stored latitude and longitude data from crime_alerts_users table
    Used primarily to calculate distance from crimes in get_recent_crime_data
    '''
    
    engine = sqlalchemy.create_engine('postgresql://postgres@localhost:5432/postgres')
    conn = engine.connect()
    select_statement = "select * from crime_alerts_users where user_name = '" + user_name + "'"
    
    select_user_data = pd.read_sql(select_statement, conn)    
    return select_user_data 

def send_alert(phone_number,message):
    
    engine = sqlalchemy.create_engine('postgresql://postgres@localhost:5432/postgres')
    conn = engine.connect()
    
    config = ConfigParser.ConfigParser()
    config.read("credentials.cfg")
    aws_key_id = config.get('credentials', 'key_id')
    aws_secret_id = config.get('credentials', 'secret_id')
    
    client = boto3.client(
        "sns",
        aws_access_key_id=aws_key_id,
        aws_secret_access_key=aws_secret_id,
        region_name="us-east-1"
    )
    
    client.publish(
        PhoneNumber = "+"+str(phone_number),
        Message=message
    )

def crime_alerts_cmd():
    '''
    Place holder for docstrings
    '''
    parser = argparse.ArgumentParser(description="run crime_alerts_main")
    parser.add_argument('--upload', '-u',action='store_true',help="Upload data to database.")
    parser.add_argument('--send_message', '-sms',action='store_true',help="Send notifications for new crime incidents")

    args=parser.parse_args()

    print args.upload

    crime_alerts_main(args.upload,args.send_message)
    
#################

def main():
    crime_alerts_cmd()

#################

if __name__=="__main__":
    main()

#################    
