from lib import get_acccess, makeApiCall
from pandas.io.json import json_normalize
import pandas as pd

def get_account_info(params):
    """ Get instagram account

    API Endpoint:
    	https://graph.facebook.com/{graph-api-version}/{page-id}?access_token={your-access-token}&fields=instagram_business_account
    Returns:
    	object: data from the endpoint
    """
    endpointParams = dict()
    endpointParams['fields'] = "business_discovery.username(" + params['ig_username'] + "){username,website,name,ig_id,id,profile_picture_url" \
                               ",biography,follows_count,followers_count,media_count}"
    endpointParams['access_token'] = params['access_token']
    url = params['endpoint_base'] + params['instagram_account_id']
    return makeApiCall(url, endpointParams, params['debug'])

params = get_acccess()
params['debug'] = 'no'
response = get_account_info(params)
data = json_normalize(response['json_data']['business_discovery'])
data = data[['followers_count']]
print data
