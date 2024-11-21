import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
import numpy as np

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

def plot_time_series(df):
    """
    Create a time series plot for bid, ask, and spread.
    """
    plt.figure(figsize=(10, 6))
    plt.plot(df['timestamp'], df['bid'], label='Bid', color='blue')
    plt.plot(df['timestamp'], df['ask'], label='Ask', color='green')
    plt.plot(df['timestamp'], df['spread'], label='Spread', color='red')
    plt.xlabel('Timestamp')
    plt.ylabel('Price')
    plt.title('Time Series: Bid, Ask, Spread')
    plt.legend()
    plt.grid()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_heatmap(df):
    """
    Create a heatmap to visualize correlations between metrics.
    """
    correlation = df[['bid', 'ask', 'spread', 'mid_price']].corr()
    plt.figure(figsize=(8, 6))
    plt.imshow(correlation, cmap='coolwarm', interpolation='none')
    plt.colorbar()
    plt.xticks(range(len(correlation.columns)), correlation.columns, rotation=45)
    plt.yticks(range(len(correlation.columns)), correlation.columns)
    plt.title('Heatmap: Correlation Between Metrics')
    plt.tight_layout()
    plt.show()

def plot_polar_chart(df):
    """
    Create a circular (polar) bar chart for the metrics.
    """
    polar_data = df.groupby('pair', as_index=False)['bid'].mean()
    labels = polar_data['pair']
    values = polar_data['bid']
    
    # Convert to radians
    angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
    
    # Plot
    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw={'projection': 'polar'})
    ax.bar(angles, values, color=plt.cm.viridis(values / max(values)), alpha=0.8)
    
    # Add labels
    ax.set_xticks(angles)
    ax.set_xticklabels(labels)
    ax.set_title('Circular Polar Chart: Average Bid Prices by Pair', va='bottom')
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    # Fetch data from the database
    df = fetch_data_from_db()
    
    # Check if data is available for visualization
    if not df.empty:
        plot_time_series(df)  # Time Series Plot
        plot_heatmap(df)  # Heatmap
        plot_polar_chart(df)  # Circular (Polar) Chart
    else:
        print("No data available for visualization.")