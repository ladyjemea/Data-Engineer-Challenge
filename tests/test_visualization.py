import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import matplotlib.pyplot as plt
from src.visualization import fetch_data_from_db, plot_time_series, plot_heatmap, plot_polar_chart


class TestVisualization(unittest.TestCase):

    def setUp(self):
        """
        Set up mock data for testing visualizations.
        """
        self.mock_data = pd.DataFrame({
            'timestamp': ['2023-11-20 10:00:00', '2023-11-20 10:05:00', '2023-11-20 10:10:00'],
            'pair': ['BTC-USD', 'ETH-USD', 'BTC-USD'],
            'bid': [50000, 1800, 51000],
            'ask': [50200, 1850, 51500],
            'spread': [200, 50, 500],
            'mid_price': [50100, 1825, 51250]
        })
        self.mock_data['timestamp'] = pd.to_datetime(self.mock_data['timestamp'])

    @patch('src.visualization.psycopg2.connect')
    def test_fetch_data_from_db(self, mock_connect):
        """
        Test that data is fetched from the database and returned as a DataFrame.
        """
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Simulate query execution and result fetching
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_cursor.fetchall.return_value = [
            ('2023-11-20 10:00:00', 'BTC-USD', 50000, 50200, 200, 50100),
            ('2023-11-20 10:05:00', 'ETH-USD', 1800, 1850, 50, 1825)
        ]
        mock_cursor.description = [
            ('timestamp',), ('pair',), ('bid',), ('ask',), ('spread',), ('mid_price',)
        ]

        df = fetch_data_from_db()

        # Verify the DataFrame is correctly fetched and structured
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertListEqual(list(df.columns), ['timestamp', 'pair', 'bid', 'ask', 'spread', 'mid_price'])

    @patch('matplotlib.pyplot.show')
    def test_plot_time_series(self, mock_show):
        """
        Test the time series plot function to ensure it runs without errors.
        """
        try:
            plot_time_series(self.mock_data)
        except Exception as e:
            self.fail(f"plot_time_series raised an exception: {e}")

        # Verify that plt.show() was called
        mock_show.assert_called_once()

    @patch('matplotlib.pyplot.show')
    def test_plot_heatmap(self, mock_show):
        """
        Test the heatmap plot function to ensure it runs without errors.
        """
        try:
            plot_heatmap(self.mock_data)
        except Exception as e:
            self.fail(f"plot_heatmap raised an exception: {e}")

        # Verify that plt.show() was called
        mock_show.assert_called_once()

    @patch('matplotlib.pyplot.show')
    def test_plot_polar_chart(self, mock_show):
        """
        Test the polar chart plot function to ensure it runs without errors.
        """
        try:
            plot_polar_chart(self.mock_data)
        except Exception as e:
            self.fail(f"plot_polar_chart raised an exception: {e}")

        # Verify that plt.show() was called
        mock_show.assert_called_once()


if __name__ == '__main__':
    unittest.main()
