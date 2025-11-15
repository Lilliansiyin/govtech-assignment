import pytest
import logging
from unittest.mock import patch, MagicMock

from src.utils.logger import StructuredLogger


def test_logger_init():
    logger = StructuredLogger("test_logger")
    assert logger.logger.name == "test_logger"
    assert logger.logger.level == logging.INFO


def test_logger_init_custom_level():
    logger = StructuredLogger("test_logger", "DEBUG")
    assert logger.logger.level == logging.DEBUG


def test_logger_info():
    logger = StructuredLogger("test_logger")
    with patch.object(logger.logger, 'info') as mock_info:
        logger.info("Test message", key="value")
        mock_info.assert_called_once()
        call_args = mock_info.call_args[0][0]
        assert "Test message" in call_args
        assert "key" in call_args


def test_logger_error():
    logger = StructuredLogger("test_logger")
    with patch.object(logger.logger, 'error') as mock_error:
        logger.error("Error message", error_code=500)
        mock_error.assert_called_once()
        call_args = mock_error.call_args[0][0]
        assert "Error message" in call_args


def test_logger_warning():
    logger = StructuredLogger("test_logger")
    with patch.object(logger.logger, 'warning') as mock_warning:
        logger.warning("Warning message", warning_type="test")
        mock_warning.assert_called_once()


def test_logger_debug():
    logger = StructuredLogger("test_logger", "DEBUG")
    with patch.object(logger.logger, 'debug') as mock_debug:
        logger.debug("Debug message", debug_info="test")
        mock_debug.assert_called_once()


def test_logger_log_method():
    logger = StructuredLogger("test_logger")
    with patch.object(logger.logger, 'info') as mock_info:
        logger.log("INFO", "Custom log", custom_field="value")
        mock_info.assert_called_once()
        call_args = mock_info.call_args[0][0]
        assert "Custom log" in call_args
        assert "custom_field" in call_args


def test_logger_multiple_handlers():
    logger1 = StructuredLogger("test1")
    logger2 = StructuredLogger("test2")
    assert logger1.logger != logger2.logger

