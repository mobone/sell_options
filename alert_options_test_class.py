import pandas as pd
from sklearn import svm
from sklearn.model_selection import train_test_split
import itertools
from sklearn.preprocessing import normalize
from random import shuffle
from redis_queue_class import RedisQueue
from multiprocessing import Process
import configparser
import pymongo
from time import sleep
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import f_regression, mutual_info_regression
from sklearn.externals import joblib
import json


file_name = 'results/143.095_5b1356d4b7eac7f0af4eb16a'

with open(file_name+'_features.txt', 'r') as f:
    model = json.loads(f.read())

model['clf'] = joblib.load(file_name+'_model.clf')



df = pd.read_csv('test_data.csv')
df['Alert Level'] = df['Change']
del df['Change']
# filter companies
if model['strike_num']:
    df = df[df['Strike Num']==model['strike_num']]
if model['Underlying_Price']:
    df = df[df['Underlying Price']<model['Underlying_Price']]
if model['alert_level']:
    df = df[df['Alert Level']>=model['alert_level']]

df = df[model['features']]
df = df.dropna()

predicted = model['clf'].predict(df)
df['Predicted'] = predicted
print(df)
print(model.keys())
print(model['features'])
