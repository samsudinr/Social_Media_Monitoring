# stopword sastrawi di link  'https://devtrik.com/python/stopword-removal-bahasa-indonesia-python-sastrawi/'
from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory
import pandas as pd
import lib
import re
import collections
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import nltk
from nltk.util import ngrams
from nltk import word_tokenize
from itertools import tee
from collections import Counter
from nltk.corpus import stopwords
from nltk.util import skipgrams
from itertools import chain
from datetime import datetime, timedelta
import time

def bigrams(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

def process_caption(raw, method, most_common_):
    delete_hashtag = ' '.join(re.sub(r"(\.\.)|#[a-zA-Z-0-9]+", " ", raw).split())
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', delete_hashtag)
    rawData = []
    for i in sentences:
        remove_punc = re.sub(r'[^\w\s]','',i, re.UNICODE)
        use_lowercase = " ".join(remove_punc.lower() for remove_punc in remove_punc.split())
        use_stopwords = " ".join(use_lowercase for use_lowercase in use_lowercase.split() if use_lowercase not in stopwordplus)
        token = nltk.word_tokenize(use_stopwords)
        if method == 1:
            uni = token
            rawData.extend(uni)
        elif method == 2:
            big = ngrams(token, 2)
            skip = list(skipgrams(token, 2, 1))
            rawData.extend(chain(big, skip))
        else:
            rawData.append("method input is false")
    return Counter(rawData).most_common(most_common_)

def process_df(dataframe, column_name):
    results = pd.DataFrame([], columns=['keywords', 'timestamp', 'id', 'like_count', 'comments_count', 'impressions', 'reach', 'saved'])
    for i in range(len(column_name)):
        df = pd.DataFrame({ 'keywords' : column_name[i]})
        df['id'] = dataframe['id'][i]
        df['timestamp'] = dataframe['timestamp'][i]
        df['like_count'] = dataframe['like_count'][i]
        df['comments_count'] = dataframe['comments_count'][i]
        df['impressions'] = dataframe['impressions'][i]
        df['reach'] = dataframe['reach'][i]
        df['saved'] = dataframe['saved'][i]
        results = results.append(df)
    results = results.reset_index(drop=False)
    return results

try:
    cred = lib.cred_emcanalyticsteam
    factory = StopWordRemoverFactory()
    en_stops = stopwords.words('english')
    id_stops = factory.get_stop_words()
    stopwords_custom = open(lib.stopwords_, "r")
    cus_stops = stopwords_custom.read().splitlines()
    more_stopwords = ['nya', 'kali', 'is', 'This']
    stopwordplus = id_stops + en_stops + cus_stops
    data = pd.read_excel(lib.rawData_media_post)
    data['keywords_unigram'] = data['caption'].apply(lambda x: process_caption(x, 1, 10))
    data['keywords_bigram'] = data['caption'].apply(lambda x: process_caption(x, 2, 10))

    df_unigram = process_df(data,data['keywords_unigram'])
    df_bigram  = process_df(data, data['keywords_bigram'])
    finale_results = df_unigram.append(df_bigram)

    finale_results['id'] = finale_results['id'].apply(str)
    finale_results['keywords'] = finale_results['keywords'].apply(lambda x: " ".join([value for value in x[0]]) if type(x[0]) is tuple else x[0] )
    finale_results['timestamp'] = finale_results['timestamp'].apply(lambda x:
                                              datetime.strftime(datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
                                                                "%Y-%m-%d %H:%M:%S"))

    finale_results['all_value'] = "(" + finale_results['id'] + ",'" + finale_results['timestamp'] + "','" + \
                                  finale_results['keywords'] + "'," + finale_results['like_count'].astype(int).astype(str) + "," + \
                                  finale_results['comments_count'].astype(int).astype(str) + "," + \
                                  finale_results['reach'].astype(int).astype(str) + "," + \
                                  finale_results['impressions'].astype(int).astype(str) + "," + \
                                  finale_results['saved'].astype(int).astype(str) + ")"

    # save all data to spreadsheet
    writer = pd.ExcelWriter(lib.rawData_ig_keywords)
    finale_results = finale_results.rename(columns={'timestamp': 'created_at', })
    finale_results_save = finale_results[['id', 'created_at', 'keywords', 'like_count', 'comments_count', 'reach',
                              'impressions', 'saved']]
    finale_results_save.to_excel(writer, sheet_name="ig_keywords", index=False)
    writer.save()
    writer.close()
    print("success save data to results")
    time.sleep(5)

    try:
        lib.upload_and_replace_file(lib.rawData_ig_keywords, lib.ID_rawData_ig_keywords, cred)
        print("Success upload to google drive")
    except Exception as e:
        print("Error upload to google drive")

    # create string value for insert query SQL
    insert_query_df = finale_results['all_value'].tolist()
    insert_query_df = ",".join(insert_query_df)
    # insert_query_df = insert_query_df.encode("utf-8")
    insert_query_df = insert_query_df.encode('ascii','ignore').decode('ascii')

    # create string value for delete query SQL
    delete_query_id = finale_results['id'].tolist()
    delete_query_id = "(" + ",".join(delete_query_id) + ")"

    # query to delete base on id
    deleteQuery = "delete from " + lib.keywordsPost + " where id in " + delete_query_id + ";"

    # query to insert new data into account_daily table
    insertQuery = "insert into " + lib.keywordsPost + " (id, created_at, keywords, like_count, comments_count, reach, impressions, saved) " \
                                                        "values " + insert_query_df +";"
    #
    #sent request to sql
    lib.connectDB(lib.host, lib.username, lib.passw, lib.db, deleteQuery, insertQuery)
    print('done')
except Exception as e:
    error = lib.ExceptionHandler()
    print(error)
    # lib.send_error_to_Discord(lib.webhook_url, str(error))
