from airflow import DAG
from airflow.decorators import task
import pandas as pd
from googleapiclient.discovery import build 
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


API_KEY = 'AIzaSyDbLD4lriT9MTpBnbPQNoB1tGm0biM--Y8'

default_args = {'owner':'geor_lolaev', 'retries':0}
CHANNEL_NAMES = ['KTV']
youtube = build(serviceName='youtube', version='v3', developerKey=API_KEY)
search_engine = youtube.search()


with DAG(default_args = default_args, catchup = False, start_date = days_ago(1), dag_id = 'youtube_dag') as dag:
    with TaskGroup(group_id = 'Extracting') as group_1:
        
        @task()
        def get_channel_ids(channel_names:list):
            channel_ids = []
            
            for channel_name in channel_names:
                search_result = search_engine.list(part='snippet', q=channel_name, type='channel').execute()
                channel_ids.append(search_result['items'][0]['snippet']['channelId'])
            return channel_ids
        channel_ids = get_channel_ids(CHANNEL_NAMES)
        @task()
        def get_main_playlist_ids(channel_ids:list):
            playlist_ids = []
            
            for channel_id in channel_ids:
                playlist_id = youtube.channels().list(part='contentDetails', id=channel_id).execute()['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                playlist_ids.append(playlist_id)
                
            return playlist_ids
        main_playlist_ids = get_main_playlist_ids(channel_ids)
        
        
        def get_all_videos_ids(playlist_id:str) -> list:
            all_videos_ids = []
            page_token = None

            while True:
                tmp = youtube.playlistItems().list(part='snippet', playlistId = playlist_id, pageToken = page_token).execute()

                for id_v in tmp['items']:
                    all_videos_ids.append(id_v['snippet']['resourceId']['videoId'])
                page_token = tmp.get('nextPageToken')
                
                if page_token is None:
                    break
            return all_videos_ids
        
        
        
        sql_create_tmp_table = SQLExecuteQueryOperator(task_id = 'create_stage_table', conn_id='postgres_default',
                                                       sql= """
                                                       create table if not exists youtube_dag.stage_video(
                                                               link text,
                                                        name_of_channel text,
                                                        name_of_video TEXT,
                                                    date_of_release TEXT,
                                                    view TEXT,
                                                    likes TEXT,
                                                    comment TEXT,
                                                    duration TEXT,
                                                    as_of_day timestamp default current_timestamp
                                                       );                                                    
                                                       """)
    with TaskGroup(group_id = 'transforming') as group_2:
        @task()
        def adding_data_to_tmp_table(ti=None):
            playlist_ids = ti.xcom_pull(key='return_value', task_ids = 'Extracting.get_main_playlist_ids')
            print(f'playlist_ids: {playlist_ids}')
            
            source = 'https://www.youtube.com/watch?v='
            
            videos_ids = []
            
            for playlist_id in playlist_ids:
                tmp = get_all_videos_ids(playlist_id)
                videos_ids = videos_ids + tmp
                
            ids = []
            names_of_channels = []
            names_of_videos = []
            dates_of_releases = []
            amounts_of_views = []
            amounts_of_likes = []
            amounts_of_comments = []
            durations = []
            g = 0
            for video in videos_ids:
                data = youtube.videos().list(part='snippet,statistics,contentDetails', id = video).execute()['items'][0]
                
                try:
                    ids_i = source+video
                    channelTitle_i = data['snippet']['channelTitle']
                    title_i = data['snippet']['title']
                    publishedAt_i = data['snippet']['publishedAt']
                    viewCount_i = data['statistics']['viewCount']
                    likeCount_i = data['statistics']['likeCount']
                    commentCount_i = data['statistics']['commentCount']
                    duration_i = data['contentDetails']['duration']
                except:
                    continue

                # айдишник видео
                ids.append(ids_i)
                names_of_channels.append(channelTitle_i)
                names_of_videos.append(title_i)
                dates_of_releases.append(publishedAt_i)
                amounts_of_views.append(viewCount_i)
                amounts_of_likes.append(likeCount_i)
                amounts_of_comments.append(commentCount_i)
                durations.append(duration_i)


            all_cols_names = ['ids',
                        'names_of_channels',
                        'names_of_videos',
                        'dates_of_releases',
                        'amounts_of_views',
                        'amounts_of_likes' ,
                        'amounts_of_comments',
                        'durations']
            
            
            all_cols = [ids,
                        names_of_channels,
                        names_of_videos,
                        dates_of_releases,
                        amounts_of_views,
                        amounts_of_likes ,
                        amounts_of_comments,
                        durations]
            
            res = {name:value for name, value in zip(all_cols_names, all_cols)}
            
            df = pd.DataFrame(res)

            ti.xcom_push(key='resulting_df', value= df)
            # df.to_csv('/opt/airflow/dags/youtube_dag/my_df.csv')
            # print(df.columns)
            # rows = [tuple(row) for row in df.to_records(index=False)]

            INSERT_QUERY = """INSERT INTO youtube_dag.stage_video(link,name_of_channel, name_of_video, date_of_release, view, likes, comment, duration) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
            print('MARKER')
            print(df.shape)
            print( df.to_records(index=False).tolist())
            hook = PostgresHook(postgres_conn_id = 'postgres_default')
            conn = hook.get_conn()
            cursor = conn.cursor()
            cursor.executemany(INSERT_QUERY, df.to_records(index=False).tolist())
            conn.commit()
            cursor.close()
            conn.close()
            
            # hook.insert_rows(table='stage_video',schema='youtube_dag', rows = my_rows #target_fields = ['link','name_of_channel','name_of_video','date_of_release','view','likes','comment','duration'],)
        add_data = adding_data_to_tmp_table()
        
        sql_create_regular_table = SQLExecuteQueryOperator(task_id = 'creating_regular_table',
                                                           conn_id = 'postgres_default',
                                                           sql = """
                                                    create table if not exists youtube_dag.video_data (
                                                        link TEXT primary key,
                                                        name_of_channel TEXT,
                                                        name_of_video TEXT,
                                                        date_of_release TEXT,
                                                        view INT,
                                                        likes INT,
                                                        comment INT,
                                                        duration REAL,
                                                        as_of_day timestamp default current_timestamp)
                                                    """
                                                    )
        
        
        @task()
        def transform_data():
            hook = PostgresHook(postgres_conn_id = 'postgres_default')
            df = hook.get_pandas_df('select * from youtube_dag.stage_video')
            
            def conversion_to_minutes(data:list):
                if len(data)==1:
                    return 1
                if len(data)==2:
                    return int(data[0])
                h_m = int(data[0]) * 60
                m = int(data[1])
                
                return h_m + m
            
            df.comment = df.comment.astype('int64', copy=True)
            df.likes = df.likes.astype('int64', copy=True)
            df.view = df.view.astype('int64', copy=True)
            df.date_of_release = pd.to_datetime(df.date_of_release)
            
            df.duration = df.duration.replace(r'[A-Z]', ' ', regex=True).str.split().map(conversion_to_minutes)
            INSERT_QUERY = """INSERT INTO YOUTUBE_DAG.VIDEO_DATA(link, name_of_channel, name_of_video, date_of_release, view, likes, comment, duration) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
            print(df.columns)
            connection = hook.get_conn()
            cursor = connection.cursor()
            cursor.executemany(INSERT_QUERY, df.drop(columns = ['as_of_day']).to_records(index=False).tolist())
            connection.commit()

            cursor.close()
            connection.close()

            # hook.insert_rows(table='video_data', rows=list(map(tuple, df.values)), target_fields=['link', 'name_of_channel', 'name_of_video', 'date_of_release', 'view', 'likes', 'comment', 'duration'],replace=True)
            
            
        transform = transform_data()
        
        # truncate_table = SQLExecuteQueryOperator(task_id = 'truncate_grouped_table',
                                                #  conn_id = 'postgres_default', sql = )
    # channel_ids >> main_playlists >> sql_create_tmp_table >> add_data >> sql_create_regular_table
    # sql_create_regular_table >> transform
    
    channel_ids >> main_playlist_ids >>sql_create_tmp_table>>add_data>>sql_create_regular_table >> transform
    
        
