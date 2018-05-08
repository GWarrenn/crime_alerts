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
import crime_alerts

def add_user(user_name,phone_number,address):
    
    geolocator = Nominatim()
    location = geolocator.geocode(address)
    
    user_df = pd.DataFrame(columns=['user_name','phone_number','user_address','user_latitude','user_longitude'])
    user_df.loc[1] = [user_name,phone_number,address,location.latitude,location.longitude]
    
    ## upload data to db
    
    engine = sqlalchemy.create_engine('postgresql://postgres@localhost:5432/postgres')
    conn = engine.connect()

    try:
        print "User data uploaded to crime_alerts_users table"
        user_df.to_sql('crime_alerts_users', conn,if_exists='append',index=False)
    except Exception as ex:
    	print 'Issue(s) with upload: row(s) not uploaded to database'
    
    ##pull last week of data for new user to avoid onslaught of initial texts
    
    print "Pulling & Uploading last week of crime data to get started (notifications not sent)"
    crime_alerts_main(send_message = False)
    
    
    	