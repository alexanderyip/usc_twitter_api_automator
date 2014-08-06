# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 11:29:43 2014

@author: ayip
"""
# for now, grab Twitter followers and etc
from twython import Twython
from datetime import date, datetime
import time
import pyodbc

db_user = 'XX'
db_pw = 'XX'
#conn_str = 'DRIVER={SQL Server};SERVER=madb;DATABASE=AnalyticsTestDB;UID='+db_user+';PWD='+db_pw
conn_str = 'DRIVER={SQL Server};SERVER=madb;DATABASE=AnalyticsTestDB;Trusted_Connection=yes'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("select distinct [date] from [dbo].[USCellular_Twitter_User]")
date_rows = cursor.fetchall()
dates = []
for d in date_rows:
    dates.append(d[0])

cursor.execute("select distinct [tweet_id] from [dbo].[USCellular_Twitter_Tweet_Info]")
tweet_rows = cursor.fetchall()
tweet_ids = []
for t in tweet_rows:
    tweet_ids.append(t[0])

today = date.today()
api_key = 'XX'
api_secret = 'XX'
twitter = Twython(api_key, api_secret, oauth_version=2)
access_token = twitter.obtain_access_token()
twitter = Twython(api_key, access_token=access_token)

screen_names = ['USCellular']
current_id = 0
max_id = 0
tweets = 0
rate_limit = 0

if today.isoformat() not in dates:
    for screen_name in screen_names:
        result = twitter.lookup_user(screen_name=screen_name)
        print 'Working on User: '+screen_name
        sql_line = 'insert into [dbo].[USCellular_Twitter_User] ([date],[id],[screen_name],[followers],[following]) values '
        sql_line += "('"+today.isoformat()+"','"+result[0]['id_str']+"','"+screen_name+"',"
        sql_line += str(result[0]['followers_count'])+','+str(result[0]['friends_count'])+')'
        cursor.execute(sql_line)
        cursor.commit()

    for screen_name in screen_names:
        while tweets < 3100:
            if max_id != 0:
                result = twitter.get_user_timeline(screen_name=screen_name,max_id=max_id-1)
            else:
                result = twitter.get_user_timeline(screen_name=screen_name)
            print 'Working on User Tweets: '+screen_name+' '+str(tweets+1)
            for r in result:
                print 'tweet_id: '+r['id_str']
                if r['id_str'] not in tweet_ids:
                    # update tweet info db
                    sql_line = 'insert into [dbo].[USCellular_Twitter_Tweet_Info] ([tweet_id],[id],[screen_name],[text],[created_at]) values '
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(r['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
                    sql_line += "('"+r['id_str']+"','"+r['user']['id_str']+"','"+screen_name+"','"
                    sql_line += r['text'].replace("'","''")+"','"+ts+"')"
                    cursor.execute(sql_line)
                    cursor.commit()
                sql_line = 'insert into [dbo].[USCellular_Twitter_Tweet_Data] ([date],[tweet_id],[retweets],[favorites]) values '
                sql_line += "('"+today.isoformat()+"','"+r['id_str']+"',"+str(r['retweet_count'])+","
                if 'favorite_count' in r.keys():
                    sql_line += str(r['favorite_count'])+")"
                else:
                    sql_line += str(r['favourite_count'])+")"
                cursor.execute(sql_line)
                cursor.commit()
                current_id = r['id']
                tweets += 1
                if current_id < max_id or max_id == 0:
                    max_id = current_id
            # check rate limit every 10th loop and wait until rate limit is reset
            rate_limit += 1
            if rate_limit%10 == 0:
                result = twitter.get_application_rate_limit_status()
                r = result['resources']['statuses']['/statuses/user_timeline']
                print 'Timeline Rate Remaining: '+str(r['remaining'])
                if r['remaining']<5:
                    reset_time = datetime.fromtimestamp(r['reset'])-datetime.today()
                    if reset_time.seconds>0:
                        print 'Rate limited! Waiting '+str(reset_time.seconds)
                        time.sleep(1+reset_time.seconds)
                r = result['resources']['application']['/application/rate_limit_status']
                print 'Rate Checker Remaining: '+str(r['remaining'])
                if r['remaining']<2:
                    reset_time = datetime.fromtimestamp(r['reset'])-datetime.today()
                    if reset_time.seconds>0:
                        print 'Rate limited! Waiting '+str(reset_time.seconds)
                        time.sleep(1+reset_time.seconds)
        tweets = 0

conn.close()
print 'Finished!'
