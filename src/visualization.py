import os
import psycopg2
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST')
}

def fetch_data():
    """Fetches data from the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = """
            SELECT timestamp, pair, bid, ask, mid_price, moving_avg, highest_bid, lowest_ask, max_spread
            FROM crypto_metrics
            ORDER BY timestamp DESC
            LIMIT 100  -- Fetch the last 100 entries
        """
        data = pd.read_sql(query, conn)
        conn.close()
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def plot_metrics(data):
    """Generates visualizations for cryptocurrency metrics."""
    if data.empty:
        print("No data available for plotting.")
        return
    
    # Convert timestamp to datetime format for easier plotting
    data['timestamp'] = pd.to_datetime(data['timestamp'])

    # Plot highest bid and lowest ask over time
    plt.figure(figsize=(12, 6))
    for pair in data['pair'].unique():
        subset = data[data['pair'] == pair]
        plt.plot(subset['timestamp'], subset['highest_bid'], label=f'Highest Bid - {pair}', linestyle='--')
        plt.plot(subset['timestamp'], subset['lowest_ask'], label=f'Lowest Ask - {pair}', linestyle=':')
    plt.title("Highest Bid and Lowest Ask Over Time")
    plt.xlabel("Timestamp")
    plt.ylabel("Price")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # Plot moving average of mid-price over time
    plt.figure(figsize=(12, 6))
    for pair in data['pair'].unique():
        subset = data[data['pair'] == pair]
        plt.plot(subset['timestamp'], subset['moving_avg'], label=f'Moving Avg - {pair}')
    plt.title("Moving Average of Mid-Price Over Time")
    plt.xlabel("Timestamp")
    plt.ylabel("Moving Average of Mid-Price")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # Plot max spread over time
    plt.figure(figsize=(12, 6))
    for pair in data['pair'].unique():
        subset = data[data['pair'] == pair]
        plt.plot(subset['timestamp'], subset['max_spread'], label=f'Max Spread - {pair}')
    plt.title("Max Spread Over Time")
    plt.xlabel("Timestamp")
    plt.ylabel("Max Spread")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    data = fetch_data()
    plot_metrics(data)