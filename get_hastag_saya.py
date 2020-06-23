import lib
import pandas as pd
import re
import time

try:
    cred = lib.cred_emcanalyticsteam
    data = pd.read_excel(lib.rawData_media_post, sheet_name='media_post')
    data = data[['id', 'timestamp', 'caption', 'like_count', 'comments_count', 'impressions', 'reach', 'saved']]
    hashtag_pattern = re.compile(r"#[a-zA-Z]+")
    hashtag_matches = data['caption'].apply(hashtag_pattern.findall)

    finale_results = pd.DataFrame([], columns=['hashtag', 'timestamp', 'id', 'like_count', 'comments_count', 'impressions', 'reach', 'saved'])
    for i in range(len(hashtag_matches)):
        df = pd.DataFrame(hashtag_matches[i], columns=['hashtag'])
        df['id'] = data['id'][i]
        df['timestamp'] = data['timestamp'][i]
        df['like_count'] = data['like_count'][i]
        df['comments_count'] = data['comments_count'][i]
        df['impressions'] = data['impressions'][i]
        df['reach'] = data['reach'][i]
        df['saved'] = data['saved'][i]
        finale_results = finale_results.append(df)
    finale_results = finale_results.reset_index(drop=False)

    finale_results['id'] = finale_results['id'].apply(str)
    finale_results['all_value'] = "(" + finale_results['id'] + ",'" + finale_results['timestamp'] + "','" + \
                                  finale_results['hashtag'] + "'," + finale_results['like_count'].astype(int).astype(str) + "," + \
                                  finale_results['comments_count'].astype(int).astype(str) + "," + \
                                  finale_results['reach'].astype(int).astype(str) + "," + \
                                  finale_results['impressions'].astype(int).astype(str) + "," + \
                                  finale_results['saved'].astype(int).astype(str) + ")"

    # save all data to spreadsheet
    writer = pd.ExcelWriter(lib.rawData_ig_hashtag)
    finale_results = finale_results.rename(columns={'timestamp': 'created_at', })
    finale_results_save = finale_results[['id', 'created_at', 'hashtag', 'like_count', 'comments_count', 'reach',
                              'impressions', 'saved']]
    finale_results_save.to_excel(writer, sheet_name="ig_hashtag", index=False)
    writer.save()
    writer.close()
    print "success save data to results"
    time.sleep(5)

    try:
        lib.upload_and_replace_file(lib.rawData_ig_hashtag, lib.ID_rawData_ig_hashtag, cred)
        print "Success upload to google drive"
    except Exception as e:
        print "Error upload to google drive"

    # create string value for insert query SQL
    insert_query_df = finale_results['all_value'].tolist()
    insert_query_df = ",".join(insert_query_df)
    insert_query_df = insert_query_df.encode("utf-8")

    # create string value for delete query SQL
    delete_query_id = finale_results['id'].tolist()
    delete_query_id = "(" + ",".join(delete_query_id) + ")"

    # query to delete base on id
    deleteQuery = "delete from " + lib.hashtagPost + " where id in " + delete_query_id + ";"

    # query to insert new data into account_daily table
    insertQuery = "insert into " + lib.hashtagPost + " (id, created_at, hashtag, like_count, comments_count, reach, impressions, saved) " \
                                                        "values " + insert_query_df +";"

    #sent request to sql
    lib.connectDB(lib.host, lib.username, lib.passw, lib.db, deleteQuery, insertQuery)
    print 'done'
except Exception as e:
    error = lib.ExceptionHandler()
    print(error)
    lib.send_error_to_Discord(lib.webhook_url, str(error))