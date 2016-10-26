import pymongo
from pymongo import MongoClient
import csv
import pandas as pd

def data_create():
     client = MongoClient()
     db = client.game
     cursor = db.ggevents.find()
     value=[]
     for doc in cursor:
           count = 0
           print "gameid"
           print(doc['bottle']['game_id'])
           print "timestamp"
           print(doc['bottle']['timestamp'])           
           print "event"
           print(doc['post']['event']) 
           print "ts"
           print(doc['post']['ts'])
           print 'ai5'
           print(doc['headers']['ai5'])
           print 'mobile version'
           print(doc['headers']['sdkv'])
           print'**********'
           value.append([doc['bottle']['game_id'],doc['bottle']['timestamp'],doc['post']['event'],doc['post']['ts'],doc['headers']['ai5'],doc['headers']['sdkv']])
     return value

def write_output(outfile, csv_data,result):
    f= open(outfile,'wb')
    csv_writer = csv.writer(f,delimiter=',')
    csv_writer.writerows(result)
    f.close()

def author_group(out_filename):
    df = pd.read_csv('Features.csv')
    df.columns=['gameid','timestamp','eventtype','ts','ai5','mobile_vesion']
    game_count = df.groupby(['gameid','timestamp','eventtype','ts','ai5','mobile_vesion']).size().reset_index()
    game_count.to_csv('count_1.csv') 


if __name__ == '__main__':

 result=data_create()
 csv_data = [['gameid','timestamp','eventtype','ts','ai5','mobile_vesion']]
 out_filename = 'Features.csv'
 write_output(out_filename, csv_data,result)
 author_group(out_filename)


 