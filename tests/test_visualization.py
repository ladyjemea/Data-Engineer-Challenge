import unittest
from unittest.mock import patch, MagicMock
from src.visualization import plot_metrics, get_data_from_db

class TestVisualization(unittest.TestCase):

    @patch('src.visualization.get_data_from_db')
    @patch('src.visualization.plt.show')
    @patch('src.visualization.plt.plot')
    def test_plot_metrics(self, mock_plot, mock_show, mock_get_data_from_db):
        """
        Test that plot_metrics retrieves data from PostgreSQL and attempts to plot it.
        """
        # Mock data to simulate database output
        mock_data = [
            {'pair': 'BTC-USD', 'timestamp': '2024-01-01 12:00:00', 'mid_price': 50050.0},
            {'pair': 'ETH-USD', 'timestamp': '2024-01-01 12:05:00', 'mid_price': 2000.0},
        ]
        mock_get_data_from_db.return_value = mock_data

        # Call the plot_metrics function
        plot_metrics(mock_data)

        # Assert that the plotting functions were called
        mock_plot.assert_called()
        mock_show.assert_called_once()

    @patch('src.visualization.psycopg2.connect')
    def test_get_data_from_db(self, mock_connect):
        """
        Test that get_data_from_db retrieves data from PostgreSQL.
        """
        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Define fake data to return from the cursor
        mock_cursor.fetchall.return_value = [
            ('BTC-USD', '2024-01-01 12:00:00', 50050.0),
            ('ETH-USD', '2024-01-01 12:05:00', 2000.0),
        ]

        # Call get_data_from_db
        result = get_data_from_db()

        # Assert the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['pair'], 'BTC-USD')
        self.assertEqual(result[0]['mid_price'], 50050.0)
        self.assertEqual(result[1]['pair'], 'ETH-USD')
        self.assertEqual(result[1]['mid_price'], 2000.0)

        # Assert the database connection and query were executed
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT pair, timestamp, mid_price FROM crypto_metrics")

if __name__ == '__main__':
    unittest.main()