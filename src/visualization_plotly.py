import plotly.express as px
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
}

def fetch_data_from_db():
    """
    Fetch all data from the database and return it as a Pandas DataFrame.
    """
    query = "SELECT * FROM crypto_metrics ORDER BY timestamp ASC"
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error fetching data from database: {e}")
        return pd.DataFrame()

def plot_interactive_time_series(df):
    """
    Create an interactive time series plot for bid, ask, and spread.
    """
    fig = px.line(
        df, 
        x='timestamp', 
        y=['bid', 'ask', 'spread'], 
        title='Interactive Time Series: Bid, Ask, Spread',
        labels={'value': 'Price', 'variable': 'Metric'},
        template='plotly_dark'
    )
    fig.show()

def plot_heatmap(df):
    """
    Create a heatmap to visualize correlations between metrics.
    """
    correlation = df[['bid', 'ask', 'spread', 'mid_price']].corr()
    fig = px.imshow(
        correlation, 
        text_auto=True, 
        title='Heatmap: Correlation Between Metrics'
    )
    fig.show()

def plot_polar_chart(df):
    """
    Create an interactive circular (polar) bar chart for the metrics.
    """
    # Group by currency pairs and calculate the average bid for simplicity
    polar_data = df.groupby('pair', as_index=False)['bid'].mean()
    fig = px.bar_polar(
        polar_data, 
        r='bid', 
        theta='pair', 
        color='pair', 
        title='Circular Polar Chart: Average Bid Prices by Pair',
        template='plotly_dark'
    )
    fig.show()

if __name__ == '__main__':
    # Fetch data from the database
    df = fetch_data_from_db()
    
    # Check if data is available for visualization
    if not df.empty:
        plot_interactive_time_series(df)  # Time Series Plot
        plot_heatmap(df)  # Heatmap
        plot_polar_chart(df)  # Circular (Polar) Chart
    else:
        print("No data available for visualization.")
        