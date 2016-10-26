import pandas as pd
import datetime
df = pd.read_csv('count_1.csv',low_memory = False)
count_valid=0
count_invalid=0
count_multiple = 0
df['timestamp'] = pd.to_datetime(df['timestamp'])
avg_time=[]
gameid=[]
stats=pd.DataFrame()
time = pd.DataFrame()
game_total=[]
game_non_cons=[]
for i in range(len(df)-1):
    row1, row2 = df.iloc[i], df.iloc[i+1]
    if((row2['ai5']==row1['ai5']) and row2['gameid']==row1['gameid']):  
          if (row1['eventtype'] == 'ggstart' and row2['eventtype']== 'ggstop'):
                 if row2['timestamp']- row1['timestamp'] >=datetime.timedelta(minutes=1): 
                      gameid.append(row2['gameid'])
                      avg_time.append(row2['timestamp']- row1['timestamp'])               
                      count_valid += 1
                 elif row2['timestamp']- row1['timestamp'] >= datetime.timedelta(seconds=30):
                      gameid.append(row2['gameid'])
                      avg_time.append(row2['timestamp']- row1['timestamp'])
                      count_multiple+=1
                 elif row2['timestamp']-row1['timestamp']<datetime.timedelta(seconds=30):
                      game_total.append(row2['gameid'])
                      count_invalid += 1
          elif (row1['eventtype'] == 'ggstart' and row2['eventtype']== 'ggstart'):
                    game_total.append(row2['gameid'])
                    count_invalid += 1
    else:
          continue
 
print('Total Valid session across all the games')          
print count_valid
print('Total inValid session across all the games')
print count_invalid
print('Total multiple session across all the games')
print count_multiple  
stats=pd.DataFrame(gameid,columns=['gameid'])
time=pd.DataFrame(avg_time,columns=['time'])
invalid_game=pd.DataFrame(game_total,columns=['gameid'])
combined=pd.concat([stats,time],axis=1)
print('Average of time for valid games')
combined['time'] = pd.to_timedelta(combined['time'])
grouped=combined.groupby(combined['gameid'])
print(grouped['time'].sum()/grouped['time'].count())
print('Total  Valid session for a game')
print(combined.groupby(['gameid']).size())
print('Total no of sessions')
game_total = pd.concat([stats,invalid_game],axis=0)
print(game_total.groupby(['gameid']).size())

