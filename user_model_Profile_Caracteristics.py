#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 23 12:29:05 2022

@author: Juan José Rincón
"""

#se importan las librerias y paquetes necesarios
import os
import pandas as pd
import pickle
import numpy as np
from collections import OrderedDict
from statistics import stdev
from collections import Counter


#se establece el directorio
dir_=os.listdir('/Users/JuanJose/Library/CloudStorage/OneDrive-UniversidaddelosAndes/Uniandes/8 Semestre/Machine Learning/Proyecto Final/Trabajo Final/Data')
os.chdir('/Users/JuanJose/Library/CloudStorage/OneDrive-UniversidaddelosAndes/Uniandes/8 Semestre/Machine Learning/Proyecto Final/Trabajo Final/Data')

# se definen funciones generales que puede
def append_value(dict_obj, key, value):
    # Check if key exist in dict or not
    if key in dict_obj:
        # Key exist in dict.
        # Check if type of value of key is list or not
        if not isinstance(dict_obj[key], list):
            # If type is not list then make it list
            dict_obj[key] = [dict_obj[key]]
        # Append the value in list
        dict_obj[key].append(value)
    else:
        # As key is not in dict,
        # so, add key-value pair
        dict_obj[key] = value
        
        
# importan los datos
data = pickle.load(open(dir_[7], "rb"))

#se crean o se modifican los types de ciertas variables
data=pd.DataFrame(data)

data=data.rename(columns ={'Replies':'Tweet Replies',
                   'Retweets':'Tweet Retweets',
                   'Favorites':'Tweet Favorites',
                   'Quotes':'Tweet Quotes'})


data['Referenced Tweet'] = data['Referenced Tweet'].fillna(0.0).astype(int)
data['Referenced Tweet Author ID'] = data['Referenced Tweet Author ID'].fillna(0.0).astype(int)
data['TweetFeature Date'] = pd.to_datetime(data['Date'])
data['TweetFeature Multimedia']=data['Media Keys'].isnull().apply(lambda x: 1 if x == False else 0)
data['Author Verified'] = data['Author Verified'].astype(int)
data['TweetFeature Retweet'] = data['is Retweet?'].astype(int)
data[['TweetFeature Quoted','TweetFeature Replied_to','TweetFeature Retweet']]=pd.get_dummies(data['Reference Type'])
data=data.drop(['Permalink','Author Location','Date','Author Description','Author Profile Image','Text','is Retweet?','Reference Type','Media URLs','Media Keys'],axis=1)


#se crea una lista de los usuarios de la BD
Usernames = set(list(data['Author Name']))


#se crean variables de la distancia en tiempo entre los tweets sigiuentes y anteriores del usuario
Data=pd.DataFrame()
i=0
for U in Usernames:
    i=i+1
    Username_Starting_info=data[data['Author Name']==U]
    Username_Starting_info=Username_Starting_info.sort_values(by='TweetFeature Date')
    Username_Starting_info['TweetFeature Distance Between Tweets']=Username_Starting_info['TweetFeature Date']-Username_Starting_info['TweetFeature Date'].shift(1)
    Data=Data.append(Username_Starting_info)
    print(i/len(Usernames))
    
  
data=Data
data=data.reset_index()
data = data.drop('index', 1) 
del Data



#### CUENTAS A LAS QUE SE LES HA MENCIONADO
Mentions=list(data.loc[data['Mentions'].notna(),'Mentions'])
Mentions = [i.split(' ')[1:] for i in Mentions] 

mentions=[]
i=0
for x in Mentions:
    for j in x:
        i=i+1
        print(i)
        mentions.append(j)
        
Mentions=mentions
del mentions
Unique_Mentions=set(list(Mentions))

#haya cuantas veces es mencionado cada usuario
d = Counter(Mentions)

Mentions=pd.DataFrame([(i, d[i]/ len(Mentions)) for i in d],columns=['UserName','Percentage'])
Mentions['Percentage']=Mentions['Percentage'].astype(float)
Mentions.sort_values(by=['Percentage'], inplace=True, ascending=False)
Mentions['accum percentage']=np.nan

i=0
for j in range(len(Mentions)):
    i=i+1
    print(j/len(Mentions))
    Mentions.iloc[j-1, 2]=sum(Mentions.iloc[0:j,1])
    
Mentions=Mentions.reset_index()
Mentions = Mentions.drop('index', 1)

#se filtran a las cuentas más relevantes
Relevant_Mentions=list(Mentions.loc[Mentions['accum percentage']<=0.5,'UserName'])





#codigo para ver cuantas veces un usuario ha mencionado a alguien relevante
data['TweetFeature MentionRelevant UserName']=0
i=0
for u in Relevant_Mentions:
    i=i+1
    L=np.column_stack([data['Mentions'].astype(str).str.contains(u, na=False)])
    j=list([i for i, x in enumerate(L) if x])
    data.loc[j,'TweetFeature MentionRelevant UserName']=1
    print(i/len(Relevant_Mentions))










#se crean las caracteristicas generales por usuario
Profile_Caracteristics=pd.DataFrame()
i=0
for U in Usernames:
    i=i+1
    Username_Starting_info=data[data['Author Name']==U]
    Username_Profile_Caracteristics= pd.DataFrame({ 
                 'User Name' : list(set(list(Username_Starting_info['Author Name']))),
                 'ID' : list(set(list(Username_Starting_info['Author ID']))),                                  
                 'Author Followers' : Username_Starting_info['Author Followers'].mean(),
                 'Author Following' : Username_Starting_info['Author Following'].mean(),
                 'Author Tweets' : Username_Starting_info['Author Tweets'].mean(),
                 'Author Verified' : Username_Starting_info['Author Verified'].mean(),
                 'Tweet Replies' : Username_Starting_info['Tweet Replies'].mean(),
                 'Tweet Retweets' : Username_Starting_info['Tweet Retweets'].mean(),
                 'Tweet Favorites' : Username_Starting_info['Tweet Favorites'].mean(),
                 'Tweet Quotes' : Username_Starting_info['Tweet Quotes'].mean(),
                 'TweetFeature First Date' : Username_Starting_info['TweetFeature Date'].min(),
                 'TweetFeature Last Date' : Username_Starting_info['TweetFeature Date'].max(),
                 'TweetFeature Date' : Username_Starting_info['TweetFeature Date'].mean(),
                 'Tweets Numbers' : Username_Starting_info['ID'].count(),
                 'TweetFeature Multimedia' : Username_Starting_info['TweetFeature Multimedia'].mean(),
                 'TweetFeature Retweet' : Username_Starting_info['TweetFeature Retweet'].mean(),
                 'TweetFeature Quoted' : Username_Starting_info['TweetFeature Quoted'].mean(),
                 'TweetFeature Reply' : Username_Starting_info['TweetFeature Replied_to'].mean(),
                 'TweetFeature MentionRelevant UserName' : Username_Starting_info['TweetFeature MentionRelevant UserName'].mean(),
                 'TweetFeature Distance Between Tweets (mean)' : Username_Starting_info['TweetFeature Distance Between Tweets'].mean(),
                 'TweetFeature Distance Between Tweets (sd)' : stdev(Username_Starting_info.loc[Username_Starting_info['TweetFeature Distance Between Tweets'].isnull()==False,'TweetFeature Distance Between Tweets'].astype(int)),
        })
    Profile_Caracteristics=Profile_Caracteristics.append(Username_Profile_Caracteristics)
    print(i/len(Usernames))
   
Profile_Caracteristics=Profile_Caracteristics.reset_index(drop=True)
    

Profile_Caracteristics.to_pickle('user_model_Profile_Caracteristics.pkl')











