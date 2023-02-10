#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 16 17:48:22 2022

@author: santiagoherreragarcia
"""

## Timelines
import numpy as np
from TwitterAPI import TwitterAPI, TwitterOAuth, TwitterRequestError, TwitterConnectionError, TwitterPager
import csv
import json
import re
import time
import os
import pandas as pd
import math as mth
from datetime import datetime
import pytz
from dateutil import parser
import sys

#5000 users: 
os.chdir('/Users/santiagoherreragarcia/OneDrive - Universidad de los Andes/Semestre 2022-1/Machine Learning/Trabajo Final/Data')
users_5000=pd.read_pickle('Usuarios Finales (5000).pkl')
users_5000=users_5000['Author ID'].drop_duplicates().astype(str).tolist()

#Function to retrieve timelines (Up to past 100 tweets)
def requests_handle(request):
    go_1s=True
    if 'title' in request.json():
        if request.json()['title']=='UsageCapExceeded':
            sys.exit("Límite mensual de 2 millones de tweets excedido :(")

    if 'x-rate-limit-reset' in request.headers and 'x-rate-limit-remaining' in request.headers:
        time_remaining=int(request.headers['x-rate-limit-reset'])
        req_remaining=int(request.headers['x-rate-limit-remaining'])
            
    #Sabemos que podemos hacer 300 requests en 15 minutos. La idea sería hacer 299 justo antes de que se reinicie. 
        if req_remaining==1:
            print("Sleep " + str(abs(time_remaining-time.time()+30)))
            time.sleep(abs(time_remaining-time.time()+30))
    if 'status' in request.json():
        print("Sleep 1s Rate Limit")
        time.sleep(3)
        go_1s=False
        
    return go_1s 
        
def main(keyword):
    global u
    global includes
    global r #Cuando toque hacer pruebas.
    global item
    timezone_bog = pytz.timezone("America/Bogota") #Nos importan los tweets con respecto a la hora en Colombia
    query = 'users/:{}/tweets'.format(keyword)
    project = 'Timeline_{}'.format(keyword)
    
    #Juancho
    twitter_app_auth = {
        'consumer_secret': 't3lZBDW4gDnzO5fWfjWkATJqApVyiee7ra8cq4T9NCGGiQYAPH',
        'consumer_key': 'pkcSs7kGX9IN66iImoOs7Jblo'
     }
    
    #Vale
    #twitter_app_auth = {
     #   'consumer_secret': '5G1gTK7WARjQg9seQBO5enkLEvBzuJ2zTn2PscOeEDxcW0fwEC',
      #  'consumer_key': 'NfNLp6obbk1TL8JUx1ke1IWcQ'
     #}
    
    users = {}
    tweets = 0
    api = TwitterAPI(twitter_app_auth['consumer_key'], twitter_app_auth['consumer_secret'], auth_type='oAuth2', api_version='2')
    next_token = ''
    go = True
    go_1s=True
    colnames_csv=['ID', 'Permalink', 'Author ID', 'Author Name', 'Author Location', 'Author Description', 'Author Followers', 'Author Following', 'Author Tweets', 'Author Profile Image', 'Author Verified','Date', 'Text', 'Replies', 'Retweets', 'Favorites', 'Quotes', 'is Retweet?', 'Reply To User Name', 'Mentions', 'Referenced Tweet', 'Reference Type', 'Referenced Tweet Author ID','Media URLs', 'Media Keys']

    try:
        os.remove(project+'.csv') 
    except OSError:
        pass
    with open(project+'.csv', 'w', encoding="utf8", newline='')  as output_file:
        writer = csv.writer(output_file)
        writer.writerow(colnames_csv)
    
    while go:
        try:
            if next_token=='':
                r = api.request(query, {
                    'max_results': 100,
                    'tweet.fields': 'created_at,public_metrics,text,author_id,entities,attachments',
                    'user.fields': 'id,verified,location,name,public_metrics,description,profile_image_url',
                    'media.fields': 'url,preview_image_url',
                    'expansions': 'author_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id,attachments.media_keys'})

                
            go_1s=requests_handle(r)
            if go_1s==False:
                continue
                    
            if 'meta' not in r.json():
                go=False
                break
            else:
                if r.json()['meta']['result_count']==0:
                    file_test=pd.read_csv(project+'.csv', nrows=1)
                    if file_test.shape[0]==0:
                        os.remove(project+'.csv')
                    go=False
                    break
            
                else:
                    includes = r.json()['includes']
                    if "tweets" in includes: 
                        includes_copy=includes["tweets"].copy()

                    for inc in includes['users']:
                        author_id = inc['id']
                        if 'location' in inc:
                            users[author_id] = [inc['username'],inc['location'].replace("/n",''),inc['description'].replace("/n",'').replace(",",'').replace('/x00',''),inc['public_metrics']['followers_count'],inc['public_metrics']['following_count'],inc['public_metrics']['tweet_count'], inc['profile_image_url'].replace("_normal",""), inc['verified']]
                        else:
                            users[author_id] = [inc['username'],None,inc['description'].replace("/n",'').replace(",",'').replace('/x00',''),inc['public_metrics']['followers_count'],inc['public_metrics']['following_count'],inc['public_metrics']['tweet_count'],inc['profile_image_url'].replace("_normal",""), inc['verified']]
                    for item in r.json()['data']:
                        media_keys=None
                        media_url=None
                        date=datetime.strptime(re.sub('.[0-9]*Z', '', item['created_at'].replace("T",' ')), '%Y-%m-%d %H:%M:%S')
                        date=pytz.utc.localize(date).astimezone(timezone_bog)
                        date=date.strftime('%Y/%m/%d %H:%M:%S')
                        u = users[item['author_id']]
                        mentions = ''
                        text=item["text"]
                        if 'entities' in item:
                            entities = item['entities']
                            #print(entities)
                            if 'mentions' in entities:
                                for en in entities['mentions']:
                                    #print(en)
                                    mentions = mentions + ' ' + en['username']
                        if 'referenced_tweets' in item: 
                            referenced_tweet_id=item['referenced_tweets'][0]['id']
                            referenced_tweet_type=item['referenced_tweets'][0]['type']
                            try:
                                index_rt=[idx for idx, included in enumerate(includes_copy) if referenced_tweet_id==included["id"]][0]
                                referenced_tweet_author_id=includes['tweets'][index_rt]['author_id']
                            except (IndexError, UnboundLocalError):
                                referenced_tweet_author_id=None

                            
                            if referenced_tweet_type=="retweeted":
                               if bool(re.search(r"(?<=RT).*?(?=:)", text)):
                                   text="RT"+re.search(r"(?<=RT).*?(?=:)", text).group(0)+": "+includes["tweets"][index_rt]["text"]
                               
                               if "attachments" in includes["tweets"][index_rt]:
                                   if "media_keys" in includes["tweets"][index_rt]["attachments"]:
                                       if 'media' in includes:
                                           media_keys=" ".join(includes["tweets"][index_rt]["attachments"]["media_keys"])
                                           media_url=" ".join([d["url"] if d["type"]=="photo" else d["preview_image_url"] for d in includes["media"] if d["media_key"] in includes["tweets"][index_rt]["attachments"]["media_keys"]])
                            else: 
                                media_keys=media_keys
                                media_url=media_url
                            
                        else: 
                            referenced_tweet_id=None
                            referenced_tweet_type=None
                            referenced_tweet_author_id=None
                            
                        if 'attachments' in item:
                            if "media_keys" in item["attachments"]:
                                if 'media' in includes:
                                    media_url= " ".join([d["url"] if d["type"]=="photo" else d["preview_image_url"] for d in includes["media"] if d["media_key"] in item["attachments"]["media_keys"]])
                                    media_keys= " ".join(item["attachments"]["media_keys"])
                        else: 
                            media_keys=media_keys
                            media_url=media_url
                        if referenced_tweet_type=="retweeted" or referenced_tweet_type=="quoted":
                            rt=True
                        else:
                            rt=False
                        
                        text=text.replace("/n",'').replace(",",'')
    
                        tweet = {
                                'ID': item['id'],
                                'Permalink': '/'+u[0]+'/status/'+item['id'],
                                'Author ID': item['author_id'],
                                'Author Name': u[0],
                                'Author Location': u[1],
                                'Author Description': str(u[2]),
                                'Author Followers': u[3],
                                'Author Following': u[4],
                                'Author Tweets': u[5],
                                'Author Profile Image': u[6],
                                'Author Verified':u[7],
                                'Date': date,
                                'Text': text,
                                'Replies': item['public_metrics']['reply_count'],
                                'Retweets': item['public_metrics']['retweet_count'],
                                'Favorites': item['public_metrics']['like_count'],
                                'Quotes':item['public_metrics']['quote_count'],
                                'is Retweet?': rt,
                                'Reply To User Name': users[item['in_reply_to_user_id']][0] if 'in_reply_to_user_id' in item and item['in_reply_to_user_id'] in users else None,
                                'Mentions': mentions,
                                'Referenced Tweet': referenced_tweet_id,
                                'Reference Type': referenced_tweet_type,
                                'Referenced Tweet Author ID':referenced_tweet_author_id,
                                'Media URLs': media_url,
                                'Media Keys':media_keys
                                
                        }
                        tweets=tweets+1
                        
                        with open(project+'.csv', 'a', encoding="utf8", newline='')  as output_file:
                            dict_writer = csv.DictWriter(output_file, tweet.keys())
                            dict_writer.writerow(tweet)
                            output_file.close()
                                
                print(str(keyword) + ' tweets: ' + str(tweets) + ' results in page: '+ str(r.json()['meta']['result_count']) +' quota: '+ str(r.get_quota()['remaining'])+ ' last tweet from: '+ date)
                if 'next_token' not in r.json()['meta']:
                    go = False
                    break
            
                next_token = ''
                go=False
                break
        except TwitterRequestError as e:
            print(e.status_code)
            for msg in iter(e):
                print(msg)

        except TwitterConnectionError as e:
            print(e)
os.chdir('/Users/santiagoherreragarcia/OneDrive - Universidad de los Andes/Semestre 2022-1/Machine Learning/Trabajo Final/Data/Timelines v1')
a=time.time()
for i in users_5000:
    main(i)
b=time.time()
print(b-a)   
    

