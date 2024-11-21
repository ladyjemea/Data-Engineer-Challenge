import unittest
from unittest.mock import patch, MagicMock, call
import psycopg2
from confluent_kafka import KafkaException
from src.error_handling import retry, connect_to_database, send_to_kafka, log_error


class TestErrorHandling(unittest.TestCase):
    """
    Unit tests for the error handling module.
    """

    def setUp(self):
        """
        Set up reusable test variables.
        """
        self.db_config = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost'
        }
        self.producer = MagicMock()
        self.kafka_topic = 'test_topic'
        self.message = '{"key": "value"}'

    def test_retry_success(self):
        """
        Test the retry decorator with a successful function execution.
        """
        @retry
        def mock_function():
            return "success"

        result = mock_function()
        self.assertEqual(result, "success")

    def test_retry_failure(self):
        """
        Test the retry decorator when all attempts fail.
        """
        @retry(retries=3, delay=1)
        def mock_function():
            raise psycopg2.OperationalError("Simulated database error")

        with self.assertRaises(psycopg2.OperationalError):
            mock_function()

    @patch("src.error_handling.psycopg2.connect")
    def test_connect_to_database_success(self, mock_connect):
        """
        Test a successful database connection using connect_to_database.
        """
        mock_connect.return_value = "mock_connection"
        result = connect_to_database(self.db_config)
        self.assertEqual(result, "mock_connection")
        mock_connect.assert_called_once_with(**self.db_config)

    @patch("src.error_handling.psycopg2.connect", side_effect=psycopg2.OperationalError("Simulated connection error"))
    def test_connect_to_database_failure(self, mock_connect):
        """
        Test connect_to_database with a failure and retry logic.
        """
        with self.assertRaises(psycopg2.OperationalError):
            connect_to_database(self.db_config)
        self.assertEqual(mock_connect.call_count, 5)  # Retries 5 times

    @patch("src.error_handling.logging.info")
    @patch("src.error_handling.logging.error")
    def test_send_to_kafka_success(self, mock_error, mock_info):
        """
        Test successful message sending to Kafka.
        """
        send_to_kafka(self.producer, self.kafka_topic, self.message)
        self.producer.produce.assert_called_once_with(self.kafka_topic, self.message)
        self.producer.flush.assert_called_once()
        mock_info.assert_any_call(f"Sending message to Kafka topic '{self.kafka_topic}': {self.message}")
        mock_info.assert_any_call(f"Message successfully sent to Kafka topic '{self.kafka_topic}'.")

    @patch("src.error_handling.logging.error")
    def test_send_to_kafka_failure(self, mock_error):
        """
        Test send_to_kafka with failure and retry logic.
        """
        self.producer.produce.side_effect = KafkaException("Simulated Kafka error")

        with self.assertRaises(KafkaException):
            send_to_kafka(self.producer, self.kafka_topic, self.message)
        
        self.assertEqual(self.producer.produce.call_count, 5)  # Retries 5 times
        mock_error.assert_any_call("Error in send_to_kafka: Simulated Kafka error")

    @patch("src.error_handling.logging.error")
    def test_log_error(self, mock_error):
        """
        Test log_error to ensure it logs the correct error message.
        """
        error_message = "Simulated error message"
        log_error(error_message)
        mock_error.assert_called_once_with(error_message)


if __name__ == '__main__':
    unittest.main()