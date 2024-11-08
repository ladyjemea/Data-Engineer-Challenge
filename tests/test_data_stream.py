import unittest
from unittest.mock import patch, MagicMock
import json
import time
from src.data_stream import generate_price_data, send_data, KAFKA_TOPIC

class TestDataStream(unittest.TestCase):

    def test_generate_price_data(self):
        """Test that generate_price_data creates a list of price records."""
        data = generate_price_data()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 3)  # We expect 3 currency pairs
        for record in data:
            self.assertIn("pair", record)
            self.assertIn("bid", record)
            self.assertIn("ask", record)
            self.assertIn("timestamp", record)
            self.assertIsInstance(record["bid"], float)
            self.assertIsInstance(record["ask"], float)
            self.assertIsInstance(record["timestamp"], float)

    @patch('src.data_stream.Producer')
    @patch('src.data_stream.generate_price_data')
    def test_send_data(self, mock_generate_price_data, MockProducer):
        """Test that send_data sends messages to Kafka."""
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
        
        # Create a mock producer instance
        mock_producer_instance = MockProducer.return_value
        
        # Call send_data and check if produce is called for each record
        with patch('time.sleep', return_value=None):  # Skip the sleep to speed up the test
            send_data()
        
        # Check that produce was called three times (once for each record in mock_data)
        self.assertEqual(mock_producer_instance.produce.call_count, len(mock_data))
        
        # Verify that each message sent to Kafka is in the correct format
        for call, record in zip(mock_producer_instance.produce.call_args_list, mock_data):
            args, kwargs = call
            self.assertEqual(args[0], KAFKA_TOPIC)
            self.assertEqual(json.loads(args[1].decode('utf-8')), record)
        
        # Verify that flush was called to ensure messages are delivered
        mock_producer_instance.flush.assert_called_once()

if __name__ == '__main__':
    unittest.main()