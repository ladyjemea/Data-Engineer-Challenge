import unittest
from unittest.mock import patch, MagicMock
import time
import json
from src.data_stream import generate_price_data, send_data, KAFKA_TOPIC


class TestDataStream(unittest.TestCase):

    def test_generate_price_data(self):
        """
        Test that generate_price_data creates a list of price records.
        """
        data = generate_price_data()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 3)  # Expect 3 currency pairs
        for record in data:
            self.assertIn("pair", record)
            self.assertIn("bid", record)
            self.assertIn("ask", record)
            self.assertIn("timestamp", record)
            self.assertIsInstance(record["bid"], float)
            self.assertIsInstance(record["ask"], float)
            self.assertIsInstance(record["timestamp"], float)

    @patch('src.data_stream.producer')
    @patch('src.data_stream.generate_price_data')
    def test_send_data(self, mock_generate_price_data, mock_producer):
        """
        Test that send_data sends messages to Kafka and logs the actions.
        """
        # Mock generated data
        mock_data = [
            {"pair": "BTC-USD", "bid": 50000.0, "ask": 50100.0, "timestamp": time.time()},
            {"pair": "ETH-USD", "bid": 2000.0, "ask": 2050.0, "timestamp": time.time()},
            {"pair": "LTC-USD", "bid": 300.0, "ask": 310.0, "timestamp": time.time()}
        ]
        mock_generate_price_data.return_value = mock_data

        # Call send_data with a limited number of iterations
        send_data(iterations=1)

        # Verify that produce is called for each record
        self.assertEqual(mock_producer.produce.call_count, len(mock_data))

        # Check the arguments passed to produce
        for call, record in zip(mock_producer.produce.call_args_list, mock_data):
            args, _ = call
            self.assertEqual(args[0], KAFKA_TOPIC)  # Kafka topic
            self.assertEqual(json.loads(args[1].decode('utf-8')), record)  # Message content

        # Ensure flush is called
        mock_producer.flush.assert_called_once()

    @patch('src.data_stream.send_data')
    @patch('src.data_stream.logging')
    def test_main(self, mock_logging, mock_send_data):
        """
        Test that the main block starts the data stream and logs appropriately.
        """
        # Simulate the main function
        with patch('builtins.__name__', '__main__'):
            from src.data_stream import __main__  # Trigger main block
            mock_send_data.assert_called_once()
            mock_logging.info.assert_any_call("Starting data stream...")


if __name__ == '__main__':
    unittest.main()