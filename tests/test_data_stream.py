import unittest
from unittest.mock import patch, MagicMock
import time
import json
from src.data_stream import generate_price_data, send_data_to_kafka, fetch_data, KAFKA_TOPIC


class TestDataStream(unittest.TestCase):
    """
    Unit tests for the data_stream module.
    """

    def test_generate_price_data(self):
        """
        Test that generate_price_data creates a list of price records.
        """
        data = generate_price_data()
        self.assertIsInstance(data, list)  # Check if the output is a list
        self.assertEqual(len(data), 3)  # Expect 3 currency pairs
        for record in data:
            # Verify the presence of expected keys
            self.assertIn("pair", record)
            self.assertIn("bid", record)
            self.assertIn("ask", record)
            self.assertIn("timestamp", record)

            # Verify data types
            self.assertIsInstance(record["pair"], str)
            self.assertIsInstance(record["bid"], float)
            self.assertIsInstance(record["ask"], float)
            self.assertIsInstance(record["timestamp"], float)

    @patch('src.data_stream.producer')
    def test_send_data_to_kafka(self, mock_producer):
        """
        Test that send_data_to_kafka sends messages to Kafka and logs the actions.
        """
        # Mock data to be sent to Kafka
        mock_data = [
            {"pair": "BTC-USD", "bid": 50000.0, "ask": 50100.0, "timestamp": time.time()},
            {"pair": "ETH-USD", "bid": 2000.0, "ask": 2050.0, "timestamp": time.time()},
            {"pair": "LTC-USD", "bid": 300.0, "ask": 310.0, "timestamp": time.time()}
        ]

        # Call the send_data_to_kafka function
        send_data_to_kafka(mock_data)

        # Verify that produce is called for each record
        self.assertEqual(mock_producer.produce.call_count, len(mock_data))

        # Check the arguments passed to produce
        for call, record in zip(mock_producer.produce.call_args_list, mock_data):
            args, _ = call
            self.assertEqual(args[0], KAFKA_TOPIC)  # Verify Kafka topic
            self.assertEqual(json.loads(args[1].decode('utf-8')), record)  # Verify message content

        # Ensure flush is called after sending all messages
        mock_producer.flush.assert_called_once()

    @patch('src.data_stream.fetch_data')
    @patch('src.data_stream.send_data_to_kafka')
    def test_fetch_and_send(self, mock_send_data_to_kafka, mock_fetch_data):
        """
        Test the integration of fetching data and sending it to Kafka.
        """
        # Mock fetch_data output
        mock_data = [
            {"pair": "BTC-USD", "bid": 50000.0, "ask": 50100.0, "timestamp": time.time()},
            {"pair": "ETH-USD", "bid": 2000.0, "ask": 2050.0, "timestamp": time.time()},
            {"pair": "LTC-USD", "bid": 300.0, "ask": 310.0, "timestamp": time.time()}
        ]
        mock_fetch_data.return_value = mock_data

        # Simulate a single iteration
        from src.data_stream import main
        with patch('time.sleep', return_value=None):  # Mock sleep to speed up the test
            main(iterations=1)

        # Verify fetch_data and send_data_to_kafka are called once
        mock_fetch_data.assert_called_once()
        mock_send_data_to_kafka.assert_called_once_with(mock_data)

    @patch('src.data_stream.logging')
    @patch('src.data_stream.main')
    def test_main_logging(self, mock_main, mock_logging):
        """
        Test that the main block starts the data stream and logs appropriately.
        """
        # Simulate the main function
        with patch('builtins.__name__', '__main__'):
            from src.data_stream import __main__  # Trigger main block
            mock_main.assert_called_once()
            mock_logging.info.assert_any_call("Starting data stream...")


if __name__ == '__main__':
    unittest.main()