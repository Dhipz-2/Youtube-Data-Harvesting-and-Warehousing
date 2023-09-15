import streamlit as st
import pandas as pd
import youtube_details as ny
import psycopg2
import psycopg2.extras

hostname = 'localhost'
database = 'youtube'
username = 'postgres'
pwd = ''# use your password
port_id = 5432

conn = psycopg2.connect(
    host = hostname,
    dbname = database,
    user = username,
    password = pwd,
    port = port_id
)

cur = conn.cursor()

st.set_page_config(layout="wide")
st.header("YOUTUBE DATA HARVESTING AND WAREHOUSING",divider='grey')

select = {
    'Channel Name':['techTFQ','Ken Jee','Alex the analyst','Madan Gowri','Akshat Shrivastava','Beer Biceps','Rahul Jain','Finance With Sharan','Linguamaria','Trade Achievers'],
    'Channel_ID':['UCnz-ZXXER4jOvuED5trXfEA', 
               'UCiT9RITQ9PW6BhXK0y2jaeg',
               'UC7cs8q-gJRlGwj4A8OmCmXg',  
               'UCY6KjrDBN_tIRFT_QNqQbRQ',
               'UCqW8jxh4tH1Z1sWPbkGWL4g',
               'UCPxMZIFE856tbTfdkdjzTSQ',
               'UC2MU9phoTYy5sigZCkrvwiw',
               'UCwVEhEzsjLym_u1he4XWFkg',
               'UCAQg09FkoobmLquNNoO4ulg',
               'UCzk4zJEoZMnjvpoN0HlKjHQ']
}
df = pd.DataFrame(select)
blankIndex=[''] * len(df)
df.index=blankIndex
tab1, tab2, tab3, tab4 = st.tabs(["**Data Extraction**".center(29,"\u2001"), "**Data Storage**".center(29,"\u2001"), "**Data Loading**".center(29,"\u2001"),"**Data Analysis**".center(29,"\u2001")])
with tab1:
    st.write('Copy the channel id from the below table and paste it in the Data storage page')
    st.table(df)
with tab2:
    option1 = st.text_input('**Enter a Channel Id**')
    b1 = st.button("Store the channel data in Mongodb")
    if b1:
        c_t, p_t, v_t, comment_t,youtube_data = ny.get_channel_stats(ny.youtube, channel_id=option1)
        st.subheader("Channel Table")
        st.dataframe(c_t)
        st.subheader("Playlist Table")
        st.dataframe(p_t)
        st.subheader("Video Table")
        st.dataframe(v_t)
        st.subheader("Comment Table")
        st.dataframe(comment_t)
        ny.col.insert_one(youtube_data)
with tab3:
    option2 = st.selectbox(
    '**Select Channel Name**',
    ('<select>','techTFQ','Ken Jee','Alex the analyst','Madan Gowri','Akshat Shrivastava','Beer Biceps','Rahul Jain','Finance With Sharan','Linguamaria','Trade Achievers'),key='abc')
    b2 = st.button("Store the channel data in SQL")
    if b2:
        if option2=='techTFQ':
            id = 'UCnz-ZXXER4jOvuED5trXfEA'
        elif option2=='Ken Jee':
            id='UCiT9RITQ9PW6BhXK0y2jaeg'
        elif option2=='Alex the analyst':
            id='UC7cs8q-gJRlGwj4A8OmCmXg'
        elif option2=='Madan Gowri':
            id='UCY6KjrDBN_tIRFT_QNqQbRQ'
        elif option2=='Akshat Shrivastava':
            id='UCqW8jxh4tH1Z1sWPbkGWL4g'
        elif option2=='Beer Biceps':
            id='UCPxMZIFE856tbTfdkdjzTSQ'
        elif option2=='Rahul Jain':
            id='UC2MU9phoTYy5sigZCkrvwiw'
        elif option2=='Finance With Sharan':
            id='UCwVEhEzsjLym_u1he4XWFkg'
        elif option2=='Linguamaria':
            id='UCAQg09FkoobmLquNNoO4ulg'
        elif option2=='Trade Achievers':
            id='UCzk4zJEoZMnjvpoN0HlKjHQ'
        else:
            st.write('Please select valid option...')
        c_t, p_t, v_t, comment_t,youtube_data = ny.get_channel_stats(ny.youtube, channel_id=id)
        w = pd.DataFrame(c_t)
        x = pd.DataFrame(p_t)
        y = pd.DataFrame(v_t)
        z = pd.DataFrame(comment_t)

        values1 = w.values.tolist()
        values2 = x.values.tolist()
        values3 = y.values.tolist()
        values4 = z.values.tolist()

        try:

            #cur.execute('DROP TABLE IF EXISTS channel_db')
            create_script1 = ''' CREATE TABLE IF NOT EXISTS channel_db (
                Channel_name varchar(50),
                Channel_id varchar(50) PRIMARY KEY,
                Channel_description  varchar(3000),
                Subscribers int,
                Total_video_count int,
                Total_view_count int,
                Playlist_id varchar(50),
                Channel_status varchar(50)) '''
            cur.execute(create_script1)
            cur.executemany('INSERT INTO channel_db VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', values1 )

            #cur.execute('DROP TABLE IF EXISTS playlist_db')
            create_script2 = ''' CREATE TABLE IF NOT EXISTS playlist_db (
                Playlist_id varchar(50) PRIMARY KEY,
                Channel_id varchar(50),
                Playlist_name varchar(50),
                CONSTRAINT fk_channel_id FOREIGN KEY(Channel_id) REFERENCES channel_db(Channel_id)
                )'''
            cur.execute(create_script2)
            cur.executemany('INSERT INTO playlist_db VALUES (%s, %s, %s)', values2 )

            # cur.execute('DROP TABLE IF EXISTS video_db')
            create_script3 = ''' CREATE TABLE IF NOT EXISTS video_db (
                Video_id varchar(50) PRIMARY KEY,
                Title varchar(3000),
                Description varchar(5000),
                Playlist_id varchar(50),
                Published_date  varchar(50),
                Views int,
                Likes int,
                Dislikes int,
                Duration int,
                Favorite_count int,
                Comment_count int,
                Thumbnails varchar(500)) '''
            cur.execute(create_script3)
            cur.executemany('INSERT INTO video_db VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', values3 )
            
            # cur.execute('DROP TABLE IF EXISTS comment_db')
            create_script4 = ''' CREATE TABLE IF NOT EXISTS comment_db (
                Comment_id varchar(50) PRIMARY KEY,
                Video_id varchar(50),
                Comment_text varchar(3000),
                Comment_author varchar(50),
                Comment_published_date varchar(50),
                Comment_reply_count int,
                CONSTRAINT fk_comment_id FOREIGN KEY(Video_id) REFERENCES video_db(Video_id)) '''
            cur.execute(create_script4)
            cur.executemany('INSERT INTO comment_db VALUES (%s, %s, %s, %s, %s, %s)', values4 )

        except Exception as error:
            print(error)

with tab4:
    option3 = st.selectbox(
    '**Select a question from the dropdown**',
    ('<select>',
     '1. What are the names of all the videos and their corresponding channels?',
     '2. Which channels have the most number of videos, and how many videos do they have?',
     '3. What are the top 10 most viewed videos and their respective channels?',
     '4. How many comments were made on each video, and what are their corresponding video names?',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7. What is the total number of views for each channel, and what are their corresponding channel names?',
     '8. What are the names of all the channels that have published videos in the year 2023?',
     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),key='abc1')
    
    if option3=='1. What are the names of all the videos and their corresponding channels?':
        cur.execute('select channel_db.channel_name, video_db.title from video_db right join channel_db on video_db.playlist_id = channel_db.playlist_id;')
        sql = cur.fetchall()
        df1 = pd.DataFrame(sql, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
        st.table(df1)
    elif option3=='2. Which channels have the most number of videos, and how many videos do they have?':
        cur.execute('select channel_name, total_video_count from channel_db order by total_video_count desc limit 5;')
        sql = cur.fetchall()
        df2 = pd.DataFrame(sql, columns=['Channel Name', 'Video Count']).reset_index(drop=True)
        st.table(df2)
    elif option3=='3. What are the top 10 most viewed videos and their respective channels?':
        cur.execute('select channel_db.channel_name, video_db.title, video_db.views from video_db right join channel_db on video_db.playlist_id = channel_db.playlist_id order by video_db.views desc limit 10;')
        sql = cur.fetchall()
        df3 = pd.DataFrame(sql, columns=['Channel Name', 'Video Name', 'Number of Views']).reset_index(drop=True)
        st.table(df3)
    elif option3=='4. How many comments were made on each video, and what are their corresponding video names?':
        cur.execute('select title, comment_count from video_db;')
        sql = cur.fetchall()
        df4 = pd.DataFrame(sql, columns=['Video Name', 'Number of Comment']).reset_index(drop=True)
        st.table(df4)
    elif option3=='5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cur.execute('select channel_db.channel_name, video_db.video_id, video_db.title, video_db.likes from video_db right join channel_db on video_db.playlist_id = channel_db.playlist_id order by video_db.likes desc limit 5;')
        sql = cur.fetchall()
        df5 = pd.DataFrame(sql, columns=['Channel Name', 'Video ID', 'Video Name','Number of Likes']).reset_index(drop=True)
        st.table(df5)
    elif option3=='6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cur.execute('select title, likes+dislikes as total from video_db;')
        sql = cur.fetchall()
        df6 = pd.DataFrame(sql, columns=['Video Name','Number of Likes and Dislikes']).reset_index(drop=True)
        st.table(df6)
    elif option3=='7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cur.execute('select channel_name, total_view_count from channel_db;')
        sql = cur.fetchall()
        df7 = pd.DataFrame(sql, columns=['Channel Name','Total Number of Views']).reset_index(drop=True)
        st.table(df7)
    elif option3=='8. What are the names of all the channels that have published videos in the year 2023?':
        cur.execute('''select channel_db.channel_name, video_db.video_id, video_db.title, video_db.published_date from video_db right join channel_db on video_db.playlist_id = channel_db.playlist_id where video_db.published_date like '2023%';''')
        sql = cur.fetchall()
        df8 = pd.DataFrame(sql, columns=['Channel Name','Video ID', 'Video Name','Published Date']).reset_index(drop=True)
        st.table(df8)
    elif option3=='9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cur.execute('select channel_db.channel_name, round(avg(video_db.duration),2) as avg_duration from video_db, channel_db where video_db.playlist_id = channel_db.playlist_id group by video_db.playlist_id,channel_db.channel_name;')
        sql = cur.fetchall()
        df9 = pd.DataFrame(sql, columns=['Channel Name','Average Duration(secs)']).reset_index(drop=True)
        st.table(df9)
    elif option3=='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cur.execute('select channel_db.channel_name, video_db.video_id, video_db.title, video_db.comment_count from video_db right join channel_db on video_db.playlist_id = channel_db.playlist_id order by video_db.comment_count desc limit 5;')
        sql = cur.fetchall()
        df10 = pd.DataFrame(sql, columns=['Channel Name','Video ID', 'Video Name', 'Number of Comments']).reset_index(drop=True)
        st.table(df10)
        
conn.commit()
conn.close()

