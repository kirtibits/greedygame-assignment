Dependency:
Mongodb,pymongo,pandas

algorithm
1)Structuring of data :
1)Restring of jsondump into mongo
2)Extracted gameid,timestamp,ai5,gamevents and grouped with the help of pandas and converted into dataframe.

Claculationg gaming session
identify device ai5 type
for each ai5 type(new gamestarted) and unique gameid(different game):
1) check if ggstrart and ggstop
    take time difference till then
    if time diff >=60
       count(valid session) 
    if time diff >=30 (multiple sessions)
        count(maintaining count of diff sessions)
    if time diff <30
        count(invalid)
    if ggstart not followed by ggstop 
        count(invalid)    
        
 Average sessiontime over valid game
 1)Grouped the valid and multiple session with respect to each game
 2)Caluclated average of timedifference obtained in each game through pandas groupby 

Total no of valid session for a game
grouped through valid session to obatin the count of no of valid sessions for each game

Total no of session of a game
concatenated all the valid and invalid gameid 
grouped through all the id to obatin the count of total sessions for each game

Scalability:

This code was implmented using pandas to use it for large scale purpose.For 100x data size, the current process which runs over a single machine can be executed over multiple nodes with the help of spark through pysspark.The current operations of filtering,counting,grouping can be effectively over multiple nodes and in later phases we can use collect function to collect analysis results across all RDDs.