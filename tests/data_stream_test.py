import unittest
from src.data_stream import fetch_data

class TestDataStream(unittest.TestCase):
    def test_fetch_data(self):
        """Test that fetch_data generates a non-empty list of records."""
        data = fetch_data()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        for record in data:
            self.assertIn("pair", record)
            self.assertIn("bid", record)
            self.assertIn("ask", record)
            self.assertIn("timestamp", record)

    # Additional tests for sending data to Kafka could be added here

if __name__ == '__main__':
    unittest.main()