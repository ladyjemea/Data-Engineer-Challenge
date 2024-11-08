import unittest
from unittest.mock import patch, MagicMock
import json
import time
import logging
from src.data_stream import generate_price_data, send_data, KAFKA_TOPIC

class TestDataStream(unittest.TestCase):

    @patch('src.data_stream.logging')
    def test_generate_price_data(self, mock_logging):
        """Test that generate_price_data creates a list of price records and logs the generated data."""
        data = generate_price_data()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 3)  # Expecting 3 currency pairs
        for record in data:
            self.assertIn("pair", record)
            self.assertIn("bid", record)
            self.assertIn("ask", record)
            self.assertIn("timestamp", record)
            self.assertIsInstance(record["bid"], float)
            self.assertIsInstance(record["ask"], float)
            self.assertIsInstance(record["timestamp"], float)
        
        # Check that logging was called with the generated data
        mock_logging.info.assert_called_with(f"Generated data: {data}")

    @patch('src.data_stream.time.sleep', return_value=None)  # Skip sleep to speed up the test
    @patch('src.data_stream.logging')
    @patch('src.data_stream.producer')
    @patch('src.data_stream.generate_price_data')
    def test_send_data(self, mock_generate_price_data, mock_producer, mock_logging, _):
        """Test that send_data sends messages to Kafka and logs the actions."""
        # Mock data to be returned by generate_price_data
        mock_data = [
            {
                "pair": "BTC-USD",
                "bid": 50000.0,
                "ask": 50100.0,
                "timestamp": time.time()
            },
            {
                "pair": "ETH-USD",
                "bid": 2000.0,
                "ask": 2050.0,
                "timestamp": time.time()
            },
            {
                "pair": "LTC-USD",
                "bid": 300.0,
                "ask": 310.0,
                "timestamp": time.time()
            }
        ]
        mock_generate_price_data.return_value = mock_data
        
        # Mock the producer's methods
        mock_producer.produce = MagicMock()
        mock_producer.flush = MagicMock()
        
        # Call send_data (it will run only once due to mocked sleep)
        with patch('builtins.input', side_effect=KeyboardInterrupt):
            try:
                send_data()
            except KeyboardInterrupt:
                pass  # Stop the infinite loop after one iteration
        
        # Check that produce was called for each record
        self.assertEqual(mock_producer.produce.call_count, len(mock_data))
        
        # Verify each message sent to Kafka
        for call_args, record in zip(mock_producer.produce.call_args_list, mock_data):
            args, kwargs = call_args
            self.assertEqual(args[0], KAFKA_TOPIC)
            sent_record = json.loads(args[1].decode('utf-8'))
            self.assertEqual(sent_record, record)
        
        # Verify that flush was called
        mock_producer.flush.assert_called_once()
        
        # Check that logging.info was called with "Sent to Kafka" messages
        expected_calls = [unittest.mock.call(f"Sent to Kafka: {record}") for record in mock_data]
        mock_logging.info.assert_has_calls(expected_calls, any_order=False)
        
        # Check that logging.info was called for "Starting data stream..."
        mock_logging.info.assert_any_call("Starting data stream...")

    @patch('src.data_stream.logging')
    def test_main(self, mock_logging):
        """Test that the main block starts the data stream and logs appropriately."""
        with patch('src.data_stream.send_data') as mock_send_data:
            with patch('builtins.input', side_effect=KeyboardInterrupt):
                try:
                    # Run the main block
                    if __name__ == '__main__':
                        send_data()
                except KeyboardInterrupt:
                    pass  # Stop after one iteration
            
            # Verify that send_data was called
            mock_send_data.assert_called_once()
            # Check that logging.info was called with "Starting data stream..."
            mock_logging.info.assert_any_call("Starting data stream...")

if __name__ == '__main__':
    unittest.main()