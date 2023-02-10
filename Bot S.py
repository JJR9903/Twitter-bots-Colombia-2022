
#Marcación de Bots usando Botometer
import botometer
import pickle
import numpy as np
import pandas as pd
import tweepy
import os

#Cuentas
os.chdir('/Users/santiagoherreragarcia/OneDrive - Universidad de los Andes/Semestre 2022-1/Machine Learning/Trabajo Final/Data')
users_5000=pd.read_pickle('Usuarios Finales (5000).pkl')
users_5000=users_5000['Author ID'].drop_duplicates().astype(int).tolist()


rapid_apis={
    'Santi':"52e56476e5msh8488e5fe8cbfb92p16172cjsn43f03990c99f", 
    'Vale': 'a56f1f380amsh25d94b62a2d8986p11b658jsn1d9f8705b566', 
    'Juancho': '4a52451bbbmsha15d3920c46fa56p13779ejsne549f18ad540'}

#Santi
twitter_app_auth_1 = {
            'consumer_key': 'bx6BGTmMDFUZtLs9WmIA6l0tr',
            'consumer_secret': 'cd2FXlFjgKfzQfXi1vbmBEq5u6iQKGaxmnPRIKi6W83FTDS1vo',
            
}

#Juancho
twitter_app_auth_2 = {
        'consumer_secret': 't3lZBDW4gDnzO5fWfjWkATJqApVyiee7ra8cq4T9NCGGiQYAPH',
        'consumer_key': 'pkcSs7kGX9IN66iImoOs7Jblo',
        'access_token':'614579513-pbGyE9Y8GjJg8fvIjenlzCb0pSw8J1fRsz8Mqzdp', 
        'access_token_secret':'xqCGfVDWgfyqG7ol6GxeGIVej0mYZ7OKZUdND1rN7xNOM'

}
    
#Vale
twitter_app_auth_3 = {
        'consumer_secret': '5G1gTK7WARjQg9seQBO5enkLEvBzuJ2zTn2PscOeEDxcW0fwEC',
        'consumer_key': 'NfNLp6obbk1TL8JUx1ke1IWcQ',
        'access_token': '1271211312304054274-BwMH0jYir423dX9lVnkGlq15VMODIp',
        'access_token_secret':'L2DO6LwQQPEBWOE3loMRzhZWzp08LdWLWBxd5PGAMapqL'
}

#Botometer Auth

bom = botometer.Botometer(wait_on_ratelimit=True,
                          rapidapi_key=rapid_apis['Santi'],
                          **twitter_app_auth_1)
#Check
def check_account1(x):
    try:
        result=bom.check_account(x)
    except tweepy.TweepError:
        result=np.nan
    return result

res={}
rows=[]
for x in users_5000[3600:]:
    print('Checking: '+str(x))
    result=check_account1(x)
    res[x]=result
    try:
        row=pd.DataFrame(pd.Series({'user':str(x),'cap':result['cap']['universal']})).transpose()
    except TypeError:
        row=pd.DataFrame(pd.Series({'user':str(x),'cap':np.nan})).transpose()
    row['bot']=np.where(row['cap']>=0.8, 1, 0)
    rows.append(row)

#Save df
df=pd.concat(rows)
df.to_pickle('Marcas 4k_5k.pkl')

#Save dict
try:
	geeky_file = open('Res 4k_5k', 'wb')
	pickle.dump(res, geeky_file)
	geeky_file.close()
except:
	print("Something went wrong")

#To read
#file_to_read = open("Res 4k_5k", "rb")
#loaded_dictionary = pickle.load(file_to_read)

