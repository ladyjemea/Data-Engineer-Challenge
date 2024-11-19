import unittest
from unittest.mock import patch, MagicMock, Mock
from src.calculations import calculate_metrics, save_metrics_to_db, consume_data
import json
import logging

class TestCalculations(unittest.TestCase):
    def setUp(self):
        # Example data to be used for testing
        self.data = {
            'pair': 'BTC-USD',
            'bid': 50000.0,
            'ask': 50100.0
        }
        # Mock database connection
        self.mock_conn = MagicMock()

    @patch('src.calculations.save_metrics_to_db')
    def test_calculate_metrics(self, mock_save_metrics):
        """Test that calculate_metrics correctly updates metrics and calls save_metrics_to_db."""
        calculate_metrics(self.data, self.mock_conn)

        mid_price = (self.data['bid'] + self.data['ask']) / 2
        spread = self.data['ask'] - self.data['bid']

        mock_save_metrics.assert_called_once()
        called_args = mock_save_metrics.call_args[0][0]
        self.assertEqual(called_args['pair'], 'BTC-USD')
        self.assertEqual(called_args['bid'], 50000.0)
        self.assertEqual(called_args['ask'], 50100.0)
        self.assertAlmostEqual(called_args['mid_price'], mid_price)
        self.assertAlmostEqual(called_args['spread'], spread)

    @patch('src.calculations.connect_to_database', return_value=MagicMock())
    def test_save_metrics_to_db(self, mock_connect):
        """Test that save_metrics_to_db inserts data into the database correctly."""
        data_to_save = {
            'pair': 'BTC-USD',
            'bid': 50000.0,
            'ask': 50100.0,
            'mid_price': 50050.0,
            'spread': 100.0,
            'highest_bid': 50000.0,
            'lowest_ask': 50100.0,
            'max_spread': 100.0
        }

        save_metrics_to_db(data_to_save, self.mock_conn)

        self.mock_conn.cursor.assert_called_once()
        cursor = self.mock_conn.cursor.return_value.__enter__.return_value
        cursor.execute.assert_called_once()

    @patch('src.calculations.Consumer')
    @patch('src.calculations.calculate_metrics')
    @patch('src.calculations.connect_to_database')
    def test_consume_data(self, mock_connect, mock_calculate_metrics, MockConsumer):
        """Test that consume_data reads from Kafka and calls calculate_metrics."""
        mock_consumer_instance = Mock()
        MockConsumer.return_value = mock_consumer_instance
        mock_consumer_instance.poll.side_effect = [
            Mock(value=json.dumps(self.data).encode('utf-8')),
            None
        ]

        mock_conn = mock_connect.return_value

        with patch('src.calculations.time.sleep', return_value=None):
            consume_data()

        mock_calculate_metrics.assert_called_once_with(self.data, mock_conn)
        mock_consumer_instance.subscribe.assert_called_once_with(['crypto_prices'])
        mock_consumer_instance.poll.assert_called()

if __name__ == '__main__':
    unittest.main()