from lib import get_acccess, makeApiCall
from pandas.io.json import json_normalize
import pandas as pd

def get_insight_account(params):
    """
    url : GET graph.facebook.com/{instagram_business_account}/insights?metric=impressions,reach,profile_views&period=day
    :param params:
    :return:
    """
    endpointsParams = dict()
    endpointsParams['metric'] = "impressions,reach,follower_count,profile_views"
    endpointsParams['period'] = "day"
    endpointsParams['access_token'] = params['access_token']
    url = params['endpoint_base'] + params['instagram_account_id'] + '/insights'
    return makeApiCall(url, endpointsParams, params['debug'])

params = get_acccess()
params['debug'] = 'no'
response = get_insight_account(params)

data = json_normalize(response['json_data']['data'])
data['tomorrow_date'] = data['values'].apply(lambda x: {x[0]['end_time'] : x[0]['value']})
data['current_date'] = data['values'].apply(lambda x: {x[1]['end_time'] : x[1]['value']})
print list(data)
data.index = pd.Index(['name','values','title','period','description','tomorrow_date', 'current_date'],name='id')
newdata = data.T
print newdata
