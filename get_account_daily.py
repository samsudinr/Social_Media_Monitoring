
from lib import get_acccess, makeApiCall
from pandas.io.json import json_normalize
from datetime import datetime, timedelta
from get_account_metrics import get_account_metrics
from get_account_info import get_account_info
import connect_db as customDB
import pandas as pd

# account daily metrics
params_metrics = get_acccess()
params_metrics['debug'] = 'no'
responseMetrics = get_account_metrics(params_metrics)

# convert account metric json to dataframe
dfMetric = pd.DataFrame(columns=['date'])
for item in responseMetrics['json_data']['data']:
    dictMetrics = {'date': [], item['name']: []}
    for value in item['values']:
        dictMetrics['date'].append(value['end_time'])
        dictMetrics[item['name']].append(value['value'])
    df = pd.DataFrame.from_dict(dictMetrics)
    dfMetric = dfMetric.append(df)
dfMetric = dfMetric.groupby('date', as_index=False).agg('sum')
dfMetric['date'] = dfMetric['date'].apply(lambda x:
                                          datetime.strftime(datetime.strptime(x, "%Y-%m-%dT%H:%M:%S+%f"),
                                                            "%Y-%m-%d"))

# account daily follower count
params = get_acccess()
params['debug'] = 'no'
response = get_account_info(params)

# convert account daily follower json to dataframe
todayFollowers_count = response['json_data']['business_discovery']['followers_count']
dfFollowers = pd.DataFrame([[datetime.now(), todayFollowers_count]], columns=['date','followers_count'])
dfFollowers['date'] = dfFollowers['date'].apply(lambda x: datetime.strftime(x,"%Y-%m-%d"))

# count followers size in yesterday
dfAccount = dfMetric.merge(dfFollowers, left_on='date', right_on='date', how='outer')
dfAccount['temp'] = dfAccount['followers_count'] - dfAccount['follower_count']
dfAccount['temp'] = dfAccount['temp'].shift(-1)
dfAccount['followers_count'] = dfAccount['followers_count'].fillna(dfAccount['temp'])
dfAccount = dfAccount.drop(columns=['follower_count','temp'])
dfAccount = dfAccount.rename(columns={'followers_count': 'follower_count'})

dfAccount['values'] = "('" + dfAccount['date'] + "'," + dfAccount['follower_count'].astype(int).astype(str) + \
                      "," + dfAccount['impressions'].astype(int).astype(str) + "," + \
                      dfAccount['reach'].astype(int).astype(str) + "," + \
                      dfAccount['profile_views'].astype(int).astype(str) + ")"

# create string date for delete query SQL
deleteDate = dfAccount['date'].astype(str).tolist()
deleteDate = "('" + "','".join(deleteDate) + "')"

# create string value for insert query SQL
insertValue = dfAccount['values'].tolist()
insertValue = ",".join(insertValue)

# query to delete last 2 days into account_daily table
deleteQuery = "delete from " + customDB.tableDailyAccount + " where id in " + deleteDate + ";"

# query to insert new data into account_daily table
insertQuery = "insert into " + customDB.tableDailyAccount + " (date,follower_count,impressions,reach,profile_views) " \
                                               "values " + insertValue + ";"

# sent request to sql
customDB.connectDB(customDB.host, customDB.username, customDB.passw, customDB.db, deleteQuery, insertQuery)