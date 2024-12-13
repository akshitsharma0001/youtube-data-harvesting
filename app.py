import streamlit as st
import pymysql
import pandas as pd
from googleapiclient.discovery import build

# YouTube API key
API_KEY = "AIzaSyBeBitE1LlxlxYcppbnTlKC-8gbw2v2dYM"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# Database connection setup
def create_connection():
    try:
        # Connecting to MySQL server 
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="akshitsharma12#",
        )
        cursor = connection.cursor()

        # Creating the database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db;")
        cursor.close()
        connection.close()

        # Connecting to the specific database
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="akshitsharma12#",
            database="youtube_data" 
        )
        return connection
    except pymysql.MySQLError as e:
        st.error(f"Database connection failed: {e}")
        return None

# Function to create necessary tables
def create_tables():
    connection = create_connection()
    if connection is None:
        return

    cursor = connection.cursor()

    # Creating Channels table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Channels (
        channel_id VARCHAR(255) PRIMARY KEY,
        channel_name VARCHAR(255),
        subscribers INT,
        total_videos INT,
        playlist_id VARCHAR(255)
    )
    """)

    # Creating Videos table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Videos (
        video_id VARCHAR(255) PRIMARY KEY,
        channel_id VARCHAR(255),
        video_title VARCHAR(255),
        views INT,
        likes INT,
        dislikes INT,
        comments INT,
        duration VARCHAR(50),
        FOREIGN KEY (channel_id) REFERENCES Channels(channel_id)
    )
    """)

    connection.commit()
    connection.close()

# Function to fetch channel details
def get_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    )
    response = request.execute()

    if "items" in response and len(response["items"]) > 0:
        channel = response["items"][0]
        return {
            "channel_name": channel["snippet"]["title"],
            "subscribers": int(channel["statistics"].get("subscriberCount", 0)),
            "total_videos": int(channel["statistics"].get("videoCount", 0)),
            "playlist_id": channel["contentDetails"]["relatedPlaylists"]["uploads"]
        }
    else:
        st.error("Invalid Channel ID or no data found.")
        return None

# Function to fetch video details
def get_video_details(youtube, playlist_id):
    videos = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    while request:
        response = request.execute()

        for item in response.get("items", []):
            video_id = item["contentDetails"]["videoId"]
            video_snippet = item["snippet"]

            video_request = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=video_id
            )
            video_response = video_request.execute()

            if video_response["items"]:
                video = video_response["items"][0]
                videos.append({
                    "video_id": video_id,
                    "title": video_snippet["title"],
                    "views": int(video["statistics"].get("viewCount", 0)),
                    "likes": int(video["statistics"].get("likeCount", 0)),
                    "dislikes": int(video["statistics"].get("dislikeCount", 0)),
                    "comments": int(video["statistics"].get("commentCount", 0)),
                    "duration": video["contentDetails"]["duration"]
                })

        request = youtube.playlistItems().list_next(request, response)

    return videos

# Function to store data in the database
def store_data(channel_id, channel_details, videos):
    connection = create_connection()
    if connection is None:
        return

    cursor = connection.cursor()

    # Insert channel details
    cursor.execute(
        """
        INSERT INTO Channels (channel_id, channel_name, subscribers, total_videos, playlist_id)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE channel_name=VALUES(channel_name), subscribers=VALUES(subscribers),
        total_videos=VALUES(total_videos), playlist_id=VALUES(playlist_id)
        """,
        (
            channel_id, channel_details["channel_name"], channel_details["subscribers"],
            channel_details["total_videos"], channel_details["playlist_id"]
        )
    )

    # Insert videos
    for video in videos:
        cursor.execute(
            """
            INSERT INTO Videos (video_id, channel_id, video_title, views, likes, dislikes, comments, duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE video_title=VALUES(video_title), views=VALUES(views), likes=VALUES(likes),
            dislikes=VALUES(dislikes), comments=VALUES(comments), duration=VALUES(duration)
            """,
            (
                video["video_id"], channel_id, video["title"], video["views"],
                video["likes"], video["dislikes"], video["comments"], video["duration"]
            )
        )

    connection.commit()
    connection.close()

# Helper function to execute queries and display results
def execute_query(query, description):
    connection = create_connection()
    if connection is None:
        return

    try:
        data = pd.read_sql(query, connection)
        st.write(description)
        st.dataframe(data)
    except Exception as e:
        st.error(f"Error executing query: {e}")
    finally:
        connection.close()

# Streamlit app
def main():
    st.title("YouTube Data Harvesting and Warehousing")

    # Initialize YouTube API client
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

    # Creating tables if they don't exist
    create_tables()

    # User input for channel ID
    channel_id = st.text_input("Enter YouTube Channel ID:")
    if st.button("Fetch and Store Data"):
        if channel_id:
            channel_details = get_channel_details(youtube, channel_id)
            if channel_details:
                videos = get_video_details(youtube, channel_details["playlist_id"])
                store_data(channel_id, channel_details, videos)
                st.success("Data successfully fetched and stored!")
        else:
            st.error("Please enter a valid YouTube Channel ID.")

    # Display stored data
    if st.button("Display Stored Data"):
        st.subheader("Stored Channels and Videos")
        query = "SELECT * FROM Channels;"
        execute_query(query, "Stored Channel Data:")

        query = "SELECT * FROM Videos;"
        execute_query(query, "Stored Video Data:")

    # Help section for pre-defined queries
    st.sidebar.title("Help")
    if st.sidebar.button("Which channel has the most subscribers?"):
        query = """
        SELECT channel_name, subscribers
        FROM Channels
        WHERE subscribers = (SELECT MAX(subscribers) FROM Channels);
        """
        execute_query(query, "Channel with the most subscribers:")

    if st.sidebar.button("Which is the most liked video?"):
        query = """
        SELECT video_title, likes
        FROM Videos
        ORDER BY likes DESC
        LIMIT 1;
        """
        execute_query(query, "Most liked video:")

    if st.sidebar.button("Which is the most viewed video?"):
        query = """
        SELECT video_title, views
        FROM Videos
        ORDER BY views DESC
        LIMIT 1;
        """
        execute_query(query, "Most viewed video:")

    if st.sidebar.button("Which is the least viewed video?"):
        query = """
        SELECT video_title, views
        FROM Videos
        ORDER BY views ASC
        LIMIT 1;
        """
        execute_query(query, "Least viewed video:")

    if st.sidebar.button("Which video got the maximum comments?"):
        query = """
        SELECT video_title, comments
        FROM Videos
        ORDER BY comments DESC
        LIMIT 1;
        """
        execute_query(query, "Video with the maximum comments:")

if __name__ == "__main__":
    main()
