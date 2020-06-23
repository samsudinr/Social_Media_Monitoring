import lib
from pandas.io.json import json_normalize
import pandas as pd
import time
import re
from datetime import datetime, timedelta

cred = lib.cred_emcanalyticsteam

api_access = lib.get_api()

def convert_insight_data(raw):
    val = []
    for i in raw:
        val.append({i['name'] : str(i['values'][0]['value'])})
    return val

def get_user_media2(params, pagingUrl = ''):
    """ Get users media

    	API Endpoint:
    		https://graph.facebook.com/{graph-api-version}/{ig-user-id}/media?fields={fields}&access_token={access-token}
    	Returns:
    		object: data from the endpoint
    """
    endpointParams = dict()
    endpointParams['fields'] = 'id,media_type,username,media_url,permalink,timestamp,caption,like_count,comments_count,insights.metric(reach,impressions,saved)'
    endpointParams['access_token'] = api_access
    if ('' == pagingUrl) :
        url = params['endpoint_base'] + params['instagram_account_id'] + '/media'
    else:
        url = pagingUrl
    return lib.makeApiCall(url, endpointParams, params['debug'])

def metric_media_video(params, id_s):
    """ Get insights for a specific media id
       API Endpoint:
       	https://graph.facebook.com/{graph-api-version}/{ig-media-id}/insights?metric={metric}
       Returns:
       	object: data from the endpoint
       """
    endpointParams = dict()  # parameter to send to the endpoint
    endpointParams['metric'] = 'video_views'  # fields to get back
    endpointParams['access_token'] = api_access  # access token
    url = params['endpoint_base'] + id_s + '/insights'  # endpoint url
    return lib.makeApiCall(url, endpointParams, params['debug'])  # make the api call

params = lib.get_acccess()
params['debug'] = 'no'

response = get_user_media2(params)
# time.sleep(3)
## get more post
# page 2
# response = get_user_media2(params, response['json_data']['paging']['next'])
# time.sleep(3)
# page 3
# time.sleep(3)
# response = get_user_media2(params, response['json_data']['paging']['next'])
data1 = json_normalize(response['json_data']['data'])

data1['insight'] = data1['insights.data'].apply(lambda x: convert_insight_data(x))
data1['reach'] = data1['insight'].apply(lambda x: x[0]['reach'])
data1['impressions'] = data1['insight'].apply(lambda x: x[1]['impressions'])
data1['saved'] = data1['insight'].apply(lambda x: x[2]['saved'])

data1 = data1[['id', 'media_type', 'media_url', 'permalink','timestamp', 'caption', 'like_count', 'comments_count', 'reach', 'impressions', 'saved']]

# get id for media type == VIDEO
video = data1.loc[data1['media_type'] == 'VIDEO', 'id']

# create new df with column video views
if len(video.index.values) != 0:
    df = pd.DataFrame([])
    for i in video:
        get_data = metric_media_video(params, i)
        columns_ = ['id']
        values = [i]
        for insight in get_data['json_data']['data']:
            columns_.append(insight['title'])
            values.append(str(insight['values'][0]['value']))
        data = pd.DataFrame([values], columns=columns_)
        data = data.rename(columns={'Tayangan Video': 'video views'})
        df = df.append(data)
        time.sleep(3)
    all_data = data1.merge(df, left_on=['id'], right_on=['id'], how='left')
    all_data = all_data.fillna(0)
else:
    all_data = data1
    all_data['video views'] = 0

# change format date
all_data['timestamp'] = all_data['timestamp'].apply(lambda x:
                                          datetime.strftime(datetime.strptime(x, "%Y-%m-%dT%H:%M:%S+%f"),
                                                            "%Y-%m-%d %H:%M:%S"))

# cleansing data caption for upload to SQL
all_data['caption'] = all_data['caption'].apply(lambda x: " ".join(x.split()))
all_data['caption'] = all_data['caption'].apply(lambda x: x.replace(',', ''))
all_data['caption'] = all_data['caption'].apply(lambda x: re.sub('[^A-Za-z0-9\#\s]+','',x))


all_data['all_value'] = "(" + all_data['id'] + ",'" + all_data['media_type'] + "','" + all_data['media_url'] + "','" \
                        + all_data['permalink'] + "','" + all_data['timestamp'] + "','" + all_data['caption'] + "'," + \
                        all_data['like_count'].astype(int).astype(str) + "," + all_data['comments_count'].astype(int).astype(str) + "," + all_data['reach'].astype(int).astype(str) + "," + \
                        all_data['impressions'].astype(int).astype(str) + "," + all_data['saved'].astype(int).astype(str) + "," \
                        + all_data['video views'].astype(int).astype(str) + ")"


# save all data to spreadsheet
writer = pd.ExcelWriter(lib.rawData_media_post)
all_data_save = all_data[['id', 'media_type', 'media_url', 'permalink', 'timestamp', 'caption', 'like_count', 'comments_count', 'reach',
                          'impressions', 'saved', 'video views']]
all_data_save.to_excel(writer, sheet_name="media_post", index=False)
writer.save()
writer.close()
print "success save data to results"

time.sleep(5)

try:
    lib.upload_and_replace_file(lib.rawData_media_post, lib.ID_rawData_media_post, cred)
    print "Success upload to google drive"
except Exception as e:
    print "Error upload to google drive"

# create string value for insert query SQL
insert_query_df = all_data['all_value'].tolist()
insert_query_df = ",".join(insert_query_df)
insert_query_df = insert_query_df.encode("utf-8")

# create string value for delete query SQL
delete_query_id = all_data['id'].tolist()
delete_query_id = "(" + ",".join(delete_query_id) + ")"

# query to delete base on id
deleteQuery = "delete from " + lib.mediaPost + " where id in " + delete_query_id + ";"

# query to insert new data into account_daily table
insertQuery = "insert into " + lib.mediaPost + " (id, media_type, media_url, link, created_at" \
                                                    ", caption, likes, comments, reach, impressions, saved" \
                                                    ", video_views) " \
                                                    "values " + insert_query_df + ";"

# sent request to sql
lib.connectDB(lib.host, lib.username, lib.passw, lib.db, deleteQuery, insertQuery)
print 'done'