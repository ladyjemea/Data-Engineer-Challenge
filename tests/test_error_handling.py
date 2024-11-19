import unittest
from unittest.mock import MagicMock, patch
from src.error_handling import retry, log_error

class TestErrorHandling(unittest.TestCase):

    @patch('time.sleep', return_value=None)
    def test_retry_decorator_success(self, mock_sleep):
        """
        Test the retry decorator succeeds after a transient error.
        """
        mock_func = MagicMock(side_effect=[Exception("Transient error"), "Success"])
        decorated_func = retry(mock_func)
        result = decorated_func()
        self.assertEqual(result, "Success")
        self.assertEqual(mock_func.call_count, 2)  # One failure + one success

    @patch('time.sleep', return_value=None)
    def test_retry_decorator_failure(self, mock_sleep):
        """
        Test the retry decorator fails after the maximum retry attempts.
        """
        mock_func = MagicMock(side_effect=Exception("Persistent error"))
        decorated_func = retry(mock_func)
        with self.assertRaises(Exception):
            decorated_func()
        self.assertEqual(mock_func.call_count, 5)  # Retry 5 times

    @patch('src.error_handling.logging.error')
    def test_log_error(self, mock_logging_error):
        """
        Test that log_error correctly logs the error message.
        """
        log_error("Test error message")
        mock_logging_error.assert_called_once_with("Test error message")

if __name__ == '__main__':
    unittest.main()