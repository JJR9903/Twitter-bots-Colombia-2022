#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 23 12:29:05 2022

@author: santiagoherreragarcia
"""

#Paste DataFrames
import pandas as pd
import numpy as np
import os


dir_=os.listdir('/Users/santiagoherreragarcia/OneDrive - Universidad de los Andes/Semestre 2022-1/Machine Learning/Trabajo Final/Data/Timelines')
dfs=[]
os.chdir('/Users/santiagoherreragarcia/OneDrive - Universidad de los Andes/Semestre 2022-1/Machine Learning/Trabajo Final/Data/Timelines')
for i in dir_:
    if 'Timeline_' in i: 
        df=pd.read_csv(i)
        
        
        
        
        dfs.append(df)

consolidated=pd.concat(dfs)

os.chdir('/Users/santiagoherreragarcia/OneDrive - Universidad de los Andes/Semestre 2022-1/Machine Learning/Trabajo Final/Data')
consolidated.to_pickle('Consolidated_V0.pkl')


#####

df = pd.read_pickle('Data/Consolidated_V0.pkl')
