import requests
import json
import time
import linecache
import mysql.connector
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import sys

ID_for_facebook_API = "1d1_TTSgIGspF-g8g3dwjh6HkucoxBYparmO0GK370oA"
file_facebook_API = "source/facebook_api.csv"

rawData_media_post = "results/rawData_media_sosial.xlsx"
ID_rawData_media_post = "1HMVsJmSypBoS2KxHLPwvukMyxZuXmZQ7Kbso2uJhyyw"

rawData_ig_keywords = "results/rawData_ig_keywords.xlsx"
ID_rawData_ig_keywords = "1sc-guvpqMheJjVxSBBY2uazvFZGYjhBXmve4ru5sFvs"
rawData_ig_hashtag = "results/rawData_ig_hashtag.xlsx"
ID_rawData_ig_hashtag = "1XTxSoGoMkUx52EVucEQDHujoOLY1QALfJ-BvFtPK8ac"

stopwords_ = "source/stopwords_ind.txt"

cred_emcanalyticsteam = {
    'pathClientSecret': 'cred/client_secret_emcanalyticsteam.json',
    'pathTokenDrive': 'cred/token_drive_emcanalyticsteam.pickle'
}

host = "54.179.162.103"
username = "rio"
passw = "rio123!"
db = "rio_toyotacommunity"
tableDailyAccount = 'account_daily'
mediaPost = 'ig_media'
keywordsPost = 'ig_keywords'
hashtagPost = 'ig_hashtag'

def test_connectDB(host, user, passw, dbNames, instquery):
    db = mysql.connector.connect(host=host, user=user, passwd=passw, database=dbNames)
    cur = db.cursor()
    # cur.execute(delquery)
    # db.commit()
    # print("Success delete data")
    # time.sleep(5)
    cur.execute(instquery)
    db.commit()
    print("Success insert new data")
    cur.close()
    db.close()

def connectDB(host, user, passw, dbNames, delquery, instquery):
    db = mysql.connector.connect(host=host, user=user, passwd=passw, database=dbNames)
    cur = db.cursor()
    cur.execute(delquery)
    db.commit()
    print("Success delete data")
    time.sleep(5)
    cur.execute(instquery)
    db.commit()
    print("Success insert new data")
    cur.close()
    db.close()


def get_acccess():
    creds = dict()
    # creds['access_token'] = 'EAAUFVYJujRsBACCdd3RtX1eiBwN2kMEUYRDJk8caiJHpGZAfBU2BcfgRbtPz8hhEzABQgpJSRgvNgaR3yGx01oEwAITqunvt9Ghy4yXn7qrDSz8g2ZBZCbPuB6ogDXUuuZBaUeqchCNniRZCTd2WE3uX79GZC3COuAPPwJhYc5IwZDZD'
    creds['client_id'] = '1413239702195483'
    creds['client_secret'] = '79407dfe22552cd96dea84b361a80d90'
    creds['graph_domain'] = 'https://graph.facebook.com/'
    creds['graph_version'] = 'v7.0'
    creds['endpoint_base'] = creds['graph_domain'] + creds['graph_version'] + '/'
    creds['debug'] = 'no'
    creds['page_id'] = '630787357334122'
    creds['instagram_account_id'] = "17841408407397253"
    creds['ig_username'] = 'toyotafacts'
    return creds

def makeApiCall( url, endpointParams, debug = 'no') :
    data = requests.get(url, endpointParams)
    response = dict()
    response['url'] = url
    response['endpoint_params'] = endpointParams
    response['endpoint_params_pretty'] = json.dumps(endpointParams, indent = 4)
    response['json_data'] = json.loads(data.content)
    response['json_data_pretty'] = json.dumps(response['json_data'], indent=4)

    if ('yes' == debug) :
        displayApiCallData(response)

    return response

def displayApiCallData(response):
    print "\nURL: "
    print response['url']
    print "\nEndpoint Params: "
    print response['endpoint_params_pretty']
    print "\nResponse: "
    print response['json_data_pretty']

def read_data_google_drive(File_ID_GoogleDrive, Title_File, dict):
    """
    function for read data from google drive,base on name ID file.
    :param File_ID_GoogleDrive: code/name id file from google drive
    :param Title_File: path + Title of file from google drive
    :return:
    """
    gauth = GoogleAuth()
    # load cline credentials with path dir, you must change the name of client_secrets.json
    gauth.LoadClientConfigFile(dict['pathClientSecret'])
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(dict['pathTokenDrive'])
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile(dict['pathTokenDrive'])

    drive = GoogleDrive(gauth)
    # read file from google drive with ID name
    File = drive.CreateFile({'id' : File_ID_GoogleDrive})
    # find out title of the file, and the mimetype
    print('title: %s, mimeType: %s' % (File['title'], File['mimeType']))
    # you can change the mimetype according the output of the mimetype file
    if '.xlsx' in Title_File:
        File.GetContentFile(Title_File, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    elif '.csv' in Title_File:
        File.GetContentFile(Title_File, mimetype='text/csv')
    else:
        File.GetContentFile(Title_File, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


def upload_and_replace_file(Directory_file, ID_file_for_replace_content_file, initDict):
    """
    :param Directory_file: Directory file local to replace content a file in google drive
    :param ID_file_for_replace_content_file: ID a file in google drive
    :return:
    """
    gauth = GoogleAuth()
    # load cline credentials with path dir, you must change the name of client_secrets.json
    gauth.LoadClientConfigFile(initDict['pathClientSecret'])
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(initDict['pathTokenDrive'])
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile(initDict['pathTokenDrive'])

    drive = GoogleDrive(gauth)
    # read file from google drive with ID name
    read_data = drive.CreateFile({'id': ID_file_for_replace_content_file})
    # function for replace content a file in google drive with content file local
    read_data.SetContentFile(Directory_file)
    read_data.Upload({'convert': True})

def ExceptionHandler():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    return 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)

def get_api():
    cred = cred_emcanalyticsteam
    # get API access > facebook graph API
    read_data_google_drive(ID_for_facebook_API, file_facebook_API, cred)
    time.sleep(5)

    graph_api = pd.read_csv(file_facebook_API)
    api_access = graph_api.iloc[0]['Access Token']
    return api_access
