import unittest
from unittest.mock import patch, MagicMock, call
from src.calculations import (
    validate_and_extract_data,
    calculate_mid_price,
    calculate_spread,
    update_cumulative_metrics,
    calculate_moving_average,
    prepare_data_for_db,
    process_message,
    save_metrics_to_db,
    consume_data,
)

import json


class TestCalculations(unittest.TestCase):
    """
    Unit tests for the calculations module.
    """

    def setUp(self):
        """
        Set up reusable variables for tests.
        """
        self.sample_valid_data = {"pair": "BTC-USD", "bid": 50000.0, "ask": 50100.0}
        self.sample_invalid_data = {"pair": "BTC-USD", "bid": "invalid_bid", "ask": 50100.0}
        self.mock_conn = MagicMock()  # Mocked database connection

    def test_validate_and_extract_data_valid(self):
        """
        Test that valid data is correctly validated and extracted.
        """
        result = validate_and_extract_data(self.sample_valid_data)
        self.assertEqual(
            result, {"pair": "BTC-USD", "bid": 50000.0, "ask": 50100.0}
        )

    def test_validate_and_extract_data_invalid(self):
        """
        Test that invalid data returns None and logs an error.
        """
        with patch("src.calculations.log_error") as mock_log_error:
            result = validate_and_extract_data(self.sample_invalid_data)
            self.assertIsNone(result)
            mock_log_error.assert_called_once_with(
                f"Malformed data: {self.sample_invalid_data}. Error: could not convert string to float: 'invalid_bid'"
            )

    def test_calculate_mid_price(self):
        """
        Test that the mid-price is calculated correctly.
        """
        result = calculate_mid_price(50000.0, 50100.0)
        self.assertEqual(result, 50050.0)

    def test_calculate_spread(self):
        """
        Test that the spread is calculated correctly.
        """
        result = calculate_spread(50000.0, 50100.0)
        self.assertEqual(result, 100.0)

    @patch("src.calculations.highest_bid", 0)
    @patch("src.calculations.lowest_ask", float("inf"))
    @patch("src.calculations.max_spread", 0)
    def test_update_cumulative_metrics(self, mock_max_spread, mock_lowest_ask, mock_highest_bid):
        """
        Test that cumulative metrics are updated correctly.
        """
        update_cumulative_metrics(50000.0, 50100.0)
        self.assertEqual(mock_highest_bid, 50000.0)
        self.assertEqual(mock_lowest_ask, 50100.0)
        self.assertEqual(mock_max_spread, 100.0)

    @patch("src.calculations.mid_prices", [])
    @patch("src.calculations.window_size", 5)
    def test_calculate_moving_average(self, mock_mid_prices, mock_window_size):
        """
        Test the moving average calculation with a window size of 5.
        """
        calculate_moving_average(50050.0)
        calculate_moving_average(50100.0)
        result = calculate_moving_average(50200.0)
        self.assertAlmostEqual(result, (50050.0 + 50100.0 + 50200.0) / 3)

    @patch("src.calculations.highest_bid", 0)
    @patch("src.calculations.lowest_ask", float("inf"))
    @patch("src.calculations.max_spread", 0)
    @patch("src.calculations.mid_prices", [])
    @patch("src.calculations.window_size", 5)
    def test_prepare_data_for_db(self, mock_mid_prices, mock_window_size, mock_max_spread, mock_lowest_ask, mock_highest_bid):
        """
        Test that prepare_data_for_db prepares data correctly for the database.
        """
        result = prepare_data_for_db("BTC-USD", 50000.0, 50100.0)
        expected = {
            "pair": "BTC-USD",
            "bid": 50000.0,
            "ask": 50100.0,
            "mid_price": 50050.0,
            "spread": 100.0,
            "highest_bid": 50000.0,
            "lowest_ask": 50100.0,
            "max_spread": 100.0,
            "moving_avg": 50050.0,
        }
        self.assertEqual(result, expected)

    @patch("src.calculations.save_metrics_to_db")
    def test_process_message_valid(self, mock_save_metrics):
        """
        Test that process_message processes valid data and saves to the database.
        """
        process_message(self.sample_valid_data, self.mock_conn)
        mock_save_metrics.assert_called_once()

    @patch("src.calculations.save_metrics_to_db")
    def test_process_message_invalid(self, mock_save_metrics):
        """
        Test that process_message skips invalid data and does not call save_metrics_to_db.
        """
        process_message(self.sample_invalid_data, self.mock_conn)
        mock_save_metrics.assert_not_called()

    @patch("src.calculations.connect_to_database", return_value=MagicMock())
    @patch("src.calculations.process_message")
    @patch("src.calculations.consumer")
    def test_consume_data(self, mock_consumer, mock_process_message, mock_connect_db):
        """
        Test the consume_data function to ensure it processes messages.
        """
        mock_message = MagicMock()
        mock_message.value.return_value = json.dumps(self.sample_valid_data).encode(
            "utf-8"
        )
        mock_consumer.poll.side_effect = [mock_message, None]  # Simulate a single valid message

        with self.assertRaises(SystemExit):  # consume_data exits if database fails
            consume_data()

        mock_process_message.assert_called_once_with(
            self.sample_valid_data, mock_connect_db.return_value
        )


if __name__ == "__main__":
    unittest.main()