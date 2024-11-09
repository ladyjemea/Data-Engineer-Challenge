import unittest
from unittest.mock import patch, MagicMock
import matplotlib.pyplot as plt
from src.visualization import plot_metrics

class TestVisualization(unittest.TestCase):
    
    @patch('src.visualization.psycopg2.connect')
    @patch('src.visualization.plt.show')
    def test_plot_metrics(self, mock_show, mock_connect):
        """
        Test that plot_metrics retrieves data from PostgreSQL and attempts to plot it.
        """
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
        mock_connect.return_value = mock_conn

        mock_data = [
            ('BTC-USD', 50000, 50100, 50050, 100, 51000, 49000, 200, 49900),
            ('ETH-USD', 2000, 2050, 2025, 50, 2100, 1950, 150, 2020),
        ]
        mock_cursor.fetchall.return_value = mock_data

        plot_metrics()

        mock_connect.assert_called_once()

        mock_show.assert_called_once()

        mock_cursor.execute.assert_called_once_with("SELECT * FROM crypto_metrics")
        self.assertTrue(mock_cursor.fetchall.called)

if __name__ == '__main__':
    unittest.main()