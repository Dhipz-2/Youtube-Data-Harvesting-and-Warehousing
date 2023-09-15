from googleapiclient.discovery import build
import pandas as pd
import pymongo
import psycopg2
import psycopg2.extras
import re

#Connecting to Mongodb
mc = pymongo.MongoClient('mongodb+srv://guvi:<password>@clusterguvi.ya4gzg5.mongodb.net/?retryWrites=true&w=majority')

api_key = 'AIzaSyDRj_ZO4GDNL695ZzKvRmsI8FkTWZ3SCiY'
youtube = build('youtube', 'v3', developerKey=api_key)

# channel_ids = ['UCnz-ZXXER4jOvuED5trXfEA', # techTFQ 
#                'UCiT9RITQ9PW6BhXK0y2jaeg', # Ken Jee
#                'UC7cs8q-gJRlGwj4A8OmCmXg', # Alex the analyst
#                'UCY6KjrDBN_tIRFT_QNqQbRQ', # Madan Gowri
#                'UCqW8jxh4tH1Z1sWPbkGWL4g', # Akshat Shrivastava
#                'UCPxMZIFE856tbTfdkdjzTSQ', # Beer Biceps
#                'UC2MU9phoTYy5sigZCkrvwiw', # Rahul Jain
#                'UCwVEhEzsjLym_u1he4XWFkg', # Finance With Sharan
#                'UCAQg09FkoobmLquNNoO4ulg', # Linguamaria
#                'UCzk4zJEoZMnjvpoN0HlKjHQ', # Trade Achievers
#                'UCh9nVJoWXmFb7sLApWGcLPQ'  #CodeBasics
#               ]

channel_id = 'UC7cs8q-gJRlGwj4A8OmCmXg'
def get_channel_stats(youtube, channel_id):

    #converting video duration into seconds
    def YTDurationToSeconds(duration):
            match = re.match('PT(\d+H)?(\d+M)?(\d+S)?', duration).groups()
            hours = _js_parseInt(match[0]) if match[0] else 0
            minutes = _js_parseInt(match[1]) if match[1] else 0
            seconds = _js_parseInt(match[2]) if match[2] else 0
            return hours * 3600 + minutes * 60 + seconds

    def _js_parseInt(string):
            return int(''.join([x for x in string if x.isdigit()]))
    
    #Fetching channel details
    request1 = youtube.channels().list(
                part='snippet,contentDetails,statistics,status',
                id=channel_id)
    response1 = request1.execute()

    data = dict(Channel_name = response1['items'][0]['snippet']['title'],
                Channel_id = channel_id,
                Channel_description = response1['items'][0]['snippet']['description'],
                Subscribers = response1['items'][0]['statistics']['subscriberCount'],
                Total_video_count = response1['items'][0]['statistics']['videoCount'],
                Total_view_count = response1['items'][0]['statistics']['viewCount'],
                Playlist_id = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                Channel_status = response1['items'][0]['status']['privacyStatus']
                )
    
    # Channel table
    c_t = dict(Channel_name = [response1['items'][0]['snippet']['title']],
                Channel_id = [channel_id],
                Channel_description = [response1['items'][0]['snippet']['description']],
                Subscribers = [response1['items'][0]['statistics']['subscriberCount']],
                Total_video_count = [response1['items'][0]['statistics']['videoCount']],
                Total_view_count = [response1['items'][0]['statistics']['viewCount']],
                Playlist_id = [response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']],
                Channel_status = [response1['items'][0]['status']['privacyStatus']]
                )
    
    #Fetching playlist details
    request = youtube.playlists().list(
        part="contentDetails, id, player, snippet,status",
        channelId=channel_id)
    response = request.execute()

    #Playlist table
    p_t = []
    for i in response['items']:
        pt_stats = dict(Playlist_id = i['id'],
                        Channel_id = i['snippet']['channelId'],
                        Playlist_name = i['snippet']['title'])
        p_t.append(pt_stats)

    #Fetching video ids
    p_id = response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    request2 = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = p_id,
                maxResults = 20)
    response2 = request2.execute()

    #Fetching video details
    videos_id = []
    for i in range(len(response2['items'])):
        videos_id.append(response2['items'][i]['contentDetails']['videoId'])
        
    request3 = youtube.videos().list(
                        part='snippet,statistics,contentDetails',
                        id=','.join(videos_id))
    response3 = request3.execute()

# The below code is to fetch all the videos under the channel

    # next_page_token = response2.get('nextPageToken')
    # more_pages = True
    
    # while more_pages:
    #     if next_page_token is None:
    #         more_pages = False
    #     else:
    #         request2 = youtube.playlistItems().list(
    #                     part='contentDetails',
    #                     playlistId = p_id,
    #                     maxResults = 50,
    #                     pageToken = next_page_token)
    #         response2 = request2.execute()
    
    #         for i in range(len(response2['items'])):
    #             videos_id.append(response2['items'][i]['contentDetails']['videoId'])
            
    #         next_page_token = response2.get('nextPageToken')

    
    # #Fetching video details
    # video_data=[]
    # v_t=[]
    # x=0
    # for k in videos_id:
    #     request3 = youtube.videos().list(
    #                     part='snippet,statistics,contentDetails',
    #                     id=k)
    #     response3 = request3.execute()

    #     video = response3['items'][0]
    #     video_stats = dict(Video_id = video['id'],
    #                        Title = video['snippet']['title'],
    #                        Description = video['snippet']['description'],
    #                        Playlist_id = ''.join(p_id),
    #                        Published_date = video['snippet']['publishedAt'],
    #                        Views = video['statistics']['viewCount'],
    #                        Likes = video['statistics']['likeCount'],
    #                        Dislikes = 0,
    #                        Duration_in_sec = video['contentDetails']['duration'],
    #                        Favorite_count = video['statistics']['favoriteCount'],
    #                        Comments_count = video['statistics']['commentCount'],
    #                        Thumbnails = video['snippet']['thumbnails']['default']['url'])
    #     if x<10:
    #         data['video'+str(x+1)] = video_stats
        
    #     v_t.append(video_stats)
    #     x=x+1

    video_data=[]
    for video in response3['items']:
        video_stats = dict(Video_id = video['id'],
                           Title = video['snippet']['title'],
                           Description = video['snippet']['description'],
                           Playlist_id = ''.join(p_id),
                           Published_date = video['snippet']['publishedAt'],
                           Views = video['statistics']['viewCount'],
                           Likes = video['statistics']['likeCount'],
                           Dislikes = 0,
                           Duration_in_sec = video['contentDetails']['duration'],
                           Favorite_count = video['statistics']['favoriteCount'],
                           Comments_count = video['statistics']['commentCount'],
                           Thumbnails = video['snippet']['thumbnails']['default']['url'])
        video_data.append(video_stats)
    
    for i in range(len(response3['items'])):
        data['video'+str(i+1)] = video_data[i]

    # Fetching comment details
    k=0
    comment_t = []
    for i in videos_id:
        request4 = youtube.commentThreads().list(part='snippet',videoId=i,maxResults = 5)
        response4 = request4.execute()

        comment_data=[]
        for j in range(len(response4['items'])):
            comment_stats=dict(Comment_id = response4['items'][j]['id'],
                               Video_id = i,
                               Comment_text = response4['items'][j]['snippet']['topLevelComment']['snippet']['textOriginal'],
                               Comment_author = response4['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                               Comment_publishedAt = response4['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt'],
                               Comment_reply_count = response4['items'][j]['snippet']['totalReplyCount'])
            comment_data.append(comment_stats)
            comment_t.append(comment_stats)  # Comment table
        for j in range(len(comment_data)):
            data['video'+str(k+1)]['comment'+str(j+1)] = comment_data[j]
        k=k+1

    #Video table
    v_t=[]
    for video in response3['items']:
        
        video_stats = dict(Video_id = video['id'],
                            Title = video['snippet']['title'],
                            Description = video['snippet']['description'],
                            Playlist_id = ''.join(p_id),
                            Published_date = video['snippet']['publishedAt'],
                            Views = video['statistics']['viewCount'],
                            Likes = video['statistics']['likeCount'],
                            Dislikes = 0,
                            Duration_in_sec = int(YTDurationToSeconds(video['contentDetails']['duration'])),
                            Favorite_count = video['statistics']['favoriteCount'],
                            Comments_count = video['statistics']['commentCount'],
                            Thumbnails = video['snippet']['thumbnails']['default']['url'])
        v_t.append(video_stats)
    return c_t, p_t, v_t, comment_t,data

c_t, p_t, v_t, comment_t,youtube_data = get_channel_stats(youtube, channel_id)

#c_t -> channel table
# p_t -> playlist table
# v_t -> video table
# youtube_data -> contain channel, video and comment details

# ------------------------------------------------------------------------------------------------------
#Connecting to Mongodb

db = mc['youtube_db']
col = db['youtube_data']

# col.insert_one(youtube_data)
