import unittest
from unittest.mock import patch, Mock, MagicMock
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
        # Set initial global metric values
        calculate_metrics(self.data, self.mock_conn)
        
        # Expected metrics
        mid_price = (self.data['bid'] + self.data['ask']) / 2
        spread = self.data['ask'] - self.data['bid']

        # Verify that save_metrics_to_db was called with the correct calculated values
        mock_save_metrics.assert_called_once()
        called_args = mock_save_metrics.call_args[0][0]  # Get the 'data' argument passed to save_metrics_to_db
        
        self.assertEqual(called_args['pair'], 'BTC-USD')
        self.assertEqual(called_args['bid'], 50000.0)
        self.assertEqual(called_args['ask'], 50100.0)
        self.assertAlmostEqual(called_args['mid_price'], mid_price)
        self.assertAlmostEqual(called_args['spread'], spread)
        
    @patch('src.calculations.connect_to_database', return_value=MagicMock())
    def test_save_metrics_to_db(self, mock_connect_to_database):
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

        # Call save_metrics_to_db with mocked connection
        save_metrics_to_db(data_to_save, self.mock_conn)

        # Check if cursor was used correctly
        self.mock_conn.cursor.assert_called_once()
        cursor = self.mock_conn.cursor.return_value.__enter__.return_value
        cursor.execute.assert_called_once_with(
            """
            INSERT INTO crypto_metrics (pair, bid, ask, mid_price, spread, highest_bid, lowest_ask, max_spread)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                data_to_save['pair'],
                data_to_save['bid'],
                data_to_save['ask'],
                data_to_save['mid_price'],
                data_to_save['spread'],
                data_to_save['highest_bid'],
                data_to_save['lowest_ask'],
                data_to_save['max_spread']
            )
        )

    @patch('src.calculations.Consumer')
    @patch('src.calculations.calculate_metrics')
    @patch('src.calculations.connect_to_database')
    def test_consume_data(self, mock_connect, mock_calculate_metrics, MockConsumer):
        """Test that consume_data reads from Kafka and calls calculate_metrics."""
        # Mock Kafka consumer
        mock_consumer_instance = Mock()
        MockConsumer.return_value = mock_consumer_instance
        mock_consumer_instance.poll.side_effect = [
            # Simulate valid messages from Kafka
            Mock(value=json.dumps(self.data).encode('utf-8')),
            None  # No more messages
        ]

        # Mock database connection
        mock_conn = mock_connect.return_value

        # Run consume_data for a short period
        with patch('src.calculations.time.sleep', return_value=None):
            consume_data()

        # Verify that calculate_metrics was called with the correct arguments
        mock_calculate_metrics.assert_called_once_with(self.data, mock_conn)

        # Verify that Kafka consumer methods were called
        mock_consumer_instance.subscribe.assert_called_once_with(['crypto_prices'])
        mock_consumer_instance.poll.assert_called()

if __name__ == '__main__':
    unittest.main()