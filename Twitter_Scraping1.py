
import snscrape.modules.twitter as sntwitter
import pandas as pd
import pymongo
import streamlit as st
from datetime import date

st.sidebar.header("Enter the scraping Data")
st.sidebar.subheader("Enter the scraping keyword : ")
hashtag = st.sidebar.text_input()
tweets_count = st.sidebar.number_input("Enter the number of Tweets : ", min_value= 1, max_value= 5000, step= 1)
st.sidebar.subheader(" Select the date ranges:")
start_date = st.sidebar.date_input("Start date  : ")
end_date = st.sidebar.date_input("End date : ")
today = str(date.today())


tweets_list = []

if hashtag:
    st.sidebar.checkbox("Start Scrape")
    

    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(f"{hashtag} since:{start_date} until:{end_date}").get_items()):
        if i >= tweets_count:
            break
        tweets_list.append([tweet.date,
                            tweet.id,
                            tweet.url,
                            tweet.rawContent,
                            tweet.user.username,
                            tweet.replyCount,
                            tweet.retweetCount,
                            tweet.likeCount,
                            tweet.lang,
                            tweet.source
                           ])
else:
    st.sidebar.checkbox("Scrape Tweets")

def data_frame(data):
    return pd.DataFrame(data, columns= ['datetime', 'user_id', 'url', 'tweet_content', 'user_name','reply_count', 'retweet_count', 'like_count', 'language', 'source'])

def convert_to_csv(c):
    return c.to_csv().encode('utf-8')


def convert_to_json(j):
    return j.to_json(orient='index')


df = data_frame(tweets_list)
csv = convert_to_csv(df)
json = convert_to_json(df)

#MongoDB connection.
client = pymongo.MongoClient("mongodb+srv://sarvancs17:Sarvan90951@guvi.jfbqhfn.mongodb.net/test")
db = client.twitterscraping
col = db.scraped_data
scr_data = {"Scraped_word" : hashtag,
           "Scraped_date" : today,
           "Scraped_data" : df.to_dict('records')
          }
if st.button("View the tweets data "):
    st.info("DataFrame viewed Successfully")
    st.write(df)

if st.button("Upload to MongoDB"):
    try:
        col.delete_many({})
        col.insert_one(scr_data)
        st.info(' Successfully uploaded to mongodb database')
    except:
        st.error('You cannot upload an empty dataset. Kindly enter the information in the leftside menu.')

#CSV Button
st.download_button(label= "Download  as a CSV",
                   data= csv,
                   file_name= 'scraped_data_twitter.csv',
                   mime= 'text/csv'
                  )
#Json Button
st.download_button(label= "Download as a JSON",
                   data= json,
                   file_name= 'scraped_tweets_data.json',
                   mime= 'text/csv'

                  )
