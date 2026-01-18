"""
Streamlit Dashboard for Movie Viewing Analytics
Real-time and batch analytics visualization
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from serving.clickhouse_client import create_client, query_data
import yaml
import os

def load_config():
    """Load configuration from config file"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

@st.cache_resource
def get_clickhouse_client():
    """Get cached ClickHouse client"""
    config = load_config()
    return create_client(config)

def get_batch_stats(client):
    """Get batch processing statistics"""
    
    user_stats_query = "SELECT * FROM batch_user_stats ORDER BY total_events DESC LIMIT 20"
    popular_movies_query = "SELECT * FROM batch_popular_movies ORDER BY view_count DESC LIMIT 20"
    ratings_query = "SELECT * FROM batch_ratings_stats WHERE rating_count > 10 ORDER BY avg_rating DESC LIMIT 20"
    hourly_query = "SELECT * FROM batch_hourly_stats ORDER BY hour"
    
    user_stats = pd.DataFrame(query_data(client, user_stats_query), 
                              columns=['user_id', 'total_events', 'movies_watched', 'avg_duration'])
    popular_movies = pd.DataFrame(query_data(client, popular_movies_query),
                                  columns=['movie_id', 'view_count'])
    ratings = pd.DataFrame(query_data(client, ratings_query),
                          columns=['movie_id', 'avg_rating', 'rating_count'])
    hourly_stats = pd.DataFrame(query_data(client, hourly_query),
                                columns=['hour', 'event_count'])
    
    return {
        'user_stats': user_stats,
        'popular_movies': popular_movies,
        'ratings': ratings,
        'hourly_stats': hourly_stats
    }

def get_stream_stats(client):
    """Get streaming statistics"""
    
    movie_views_query = """
        SELECT movie_id, SUM(view_count) as total_views 
        FROM stream_movie_views 
        WHERE window_start > now() - INTERVAL 1 HOUR
        GROUP BY movie_id 
        ORDER BY total_views DESC 
        LIMIT 20
    """
    
    event_dist_query = """
        SELECT event_type, SUM(event_count) as total_events
        FROM stream_event_distribution
        WHERE window_start > now() - INTERVAL 1 HOUR
        GROUP BY event_type
    """
    
    recent_ratings_query = """
        SELECT movie_id, avg_rating, rating_count
        FROM stream_ratings
        WHERE window_start > now() - INTERVAL 1 HOUR
        ORDER BY window_start DESC
        LIMIT 20
    """
    
    movie_views = pd.DataFrame(query_data(client, movie_views_query),
                               columns=['movie_id', 'total_views'])
    event_dist = pd.DataFrame(query_data(client, event_dist_query),
                             columns=['event_type', 'total_events'])
    recent_ratings = pd.DataFrame(query_data(client, recent_ratings_query),
                                  columns=['movie_id', 'avg_rating', 'rating_count'])
    
    return {
        'movie_views': movie_views,
        'event_distribution': event_dist,
        'recent_ratings': recent_ratings
    }

def main():
    """Main dashboard application"""
    st.set_page_config(page_title="Movie Viewing Analytics", layout="wide")
    
    st.title("Movie Viewing Behavior Analysis & Monitoring")
    st.markdown("Lambda Architecture Dashboard")
    
    try:
        client = get_clickhouse_client()
        
        tab1, tab2, tab3 = st.tabs(["Batch Analytics", "Streaming Analytics", "Overview"])
        
        with tab1:
            st.header("Batch Processing Results")
            
            batch_stats = get_batch_stats(client)
            
            if not batch_stats['user_stats'].empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Top Users by Activity")
                    st.dataframe(batch_stats['user_stats'], use_container_width=True)
                
                with col2:
                    st.subheader("Most Popular Movies")
                    st.dataframe(batch_stats['popular_movies'], use_container_width=True)
                
                col3, col4 = st.columns(2)
                
                with col3:
                    st.subheader("Top Rated Movies")
                    if not batch_stats['ratings'].empty:
                        fig = px.bar(batch_stats['ratings'], 
                                     x='movie_id', y='avg_rating',
                                     title="Average Ratings by Movie")
                        st.plotly_chart(fig, use_container_width=True)
                
                with col4:
                    st.subheader("Hourly Activity Distribution")
                    if not batch_stats['hourly_stats'].empty:
                        fig = px.line(batch_stats['hourly_stats'],
                                     x='hour', y='event_count',
                                     title="Events by Hour")
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No batch data available. Run batch processing first.")
        
        with tab2:
            st.header("Real-time Streaming Analytics")
            
            stream_stats = get_stream_stats(client)
            
            if not stream_stats['movie_views'].empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Recent Movie Views (Last Hour)")
                    fig = px.bar(stream_stats['movie_views'],
                                x='movie_id', y='total_views',
                                title="Views in Last Hour")
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    st.subheader("Event Type Distribution")
                    if not stream_stats['event_distribution'].empty:
                        fig = px.pie(stream_stats['event_distribution'],
                                     values='total_events', names='event_type',
                                     title="Event Types")
                        st.plotly_chart(fig, use_container_width=True)
                
                if not stream_stats['recent_ratings'].empty:
                    st.subheader("Recent Ratings")
                    st.dataframe(stream_stats['recent_ratings'], use_container_width=True)
            else:
                st.info("No streaming data available. Start streaming processor.")
        
        with tab3:
            st.header("System Overview")
            
            try:
                total_users_query = "SELECT COUNT(DISTINCT user_id) as total FROM batch_user_stats"
                total_movies_query = "SELECT COUNT(DISTINCT movie_id) as total FROM batch_popular_movies"
                
                total_users = query_data(client, total_users_query)[0][0]
                total_movies = query_data(client, total_movies_query)[0][0]
                
                metric1, metric2, metric3 = st.columns(3)
                
                with metric1:
                    st.metric("Total Users Analyzed", total_users if total_users else 0)
                
                with metric2:
                    st.metric("Total Movies Tracked", total_movies if total_movies else 0)
                
                with metric3:
                    st.metric("Architecture Type", "Lambda")
                
            except Exception as e:
                st.warning(f"Could not fetch overview metrics: {e}")
    
    except Exception as e:
        st.error(f"Error connecting to ClickHouse: {e}")
        st.info("Please ensure ClickHouse is running and accessible.")

if __name__ == "__main__":
    main()
