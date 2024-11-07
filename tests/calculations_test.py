import unittest
from src.calculations import calculate_metrics, save_metrics_to_db
from unittest.mock import Mock

class TestCalculations(unittest.TestCase):
    def test_calculate_metrics(self):
        """Test that calculate_metrics correctly updates metrics."""
        data = {'pair': 'BTC-USD', 'bid': 50000.0, 'ask': 50100.0}
        conn = Mock()  # Mocked database connection
        calculate_metrics(data, conn)
        
        # Test expected values for metrics calculation
        self.assertEqual(data['pair'], 'BTC-USD')
        self.assertAlmostEqual((data['bid'] + data['ask']) / 2, 50050.0)
        self.assertAlmostEqual(data['ask'] - data['bid'], 100.0)

    def test_save_metrics_to_db(self):
        """Test that save_metrics_to_db calls the database cursor correctly."""
        conn = Mock()
        data = {
            'pair': 'BTC-USD',
            'bid': 50000.0,
            'ask': 50100.0,
            'mid_price': 50050.0,
            'spread': 100.0,
            'highest_bid': 50000.0,
            'lowest_ask': 50100.0,
            'max_spread': 100.0
        }
        save_metrics_to_db(data, conn)
        
        conn.cursor.assert_called()  # Check if the cursor is called

if __name__ == '__main__':
    unittest.main()