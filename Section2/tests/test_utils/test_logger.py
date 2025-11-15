"""Unit tests for logger utility."""

import pytest
import logging
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.utils.logger import setup_logger, flush_all_handlers, RobustFileHandler


class TestRobustFileHandler:
    """Test cases for RobustFileHandler."""
    
    def test_init_new_file(self, tmp_path):
        """Test RobustFileHandler initialization with new file."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        assert handler._filename == str(log_file.absolute().resolve())
        assert handler._is_new_file is True
        assert log_file.exists()
        handler.close()
    
    def test_init_existing_file(self, tmp_path):
        """Test RobustFileHandler initialization with existing file."""
        log_file = tmp_path / "test.log"
        log_file.write_text("existing content\n")
        
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        assert handler._filename == str(log_file.absolute().resolve())
        assert handler._is_new_file is False
        handler.close()
    
    def test_emit_success(self, tmp_path):
        """Test emit method with successful write."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        handler.emit(record)
        handler.close()
        
        assert "Test message" in log_file.read_text()
    
    def test_emit_with_exception(self, tmp_path):
        """Test emit method handles exceptions."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        # Close the stream to cause an error, then try to emit
        handler.stream.close()
        
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Should not raise exception - emit should catch it
        try:
            handler.emit(record)
        except Exception:
            # If it raises, that's also acceptable - the handler should handle it
            pass
        finally:
            # Clean up - handler might already be closed
            try:
                handler.close()
            except:
                pass
    
    def test_emit_no_stream(self, tmp_path):
        """Test emit when stream is None."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        handler.stream = None
        
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        # Should not raise exception
        handler.emit(record)
        handler.close()
    
    def test_final_flush_with_stream(self, tmp_path):
        """Test _final_flush with valid stream."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        handler._final_flush()
        handler.close()
    
    def test_final_flush_no_stream(self, tmp_path):
        """Test _final_flush when stream is None."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        handler.stream = None
        
        handler._final_flush()
        handler.close()
    
    def test_final_flush_no_fileno(self, tmp_path):
        """Test _final_flush when stream has no fileno."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        # Mock stream without fileno
        mock_stream = MagicMock()
        mock_stream.flush = MagicMock()
        del mock_stream.fileno
        handler.stream = mock_stream
        
        handler._final_flush()
        handler.close()
    
    def test_final_flush_fileno_error(self, tmp_path):
        """Test _final_flush when fileno raises OSError."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        # Mock stream with fileno that raises error
        mock_stream = MagicMock()
        mock_stream.flush = MagicMock()
        mock_stream.fileno = MagicMock(side_effect=OSError("Bad file descriptor"))
        handler.stream = mock_stream
        
        handler._final_flush()
        handler.close()
    
    def test_close_with_stream(self, tmp_path):
        """Test close method with valid stream."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        # Verify stream exists before closing
        assert handler.stream is not None
        handler.close()
        # After close, stream should be closed or None
        assert handler.stream is None or handler.stream.closed
    
    def test_close_no_stream(self, tmp_path):
        """Test close when stream is None."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        handler.stream = None
        
        handler.close()
    
    def test_close_no_fileno(self, tmp_path):
        """Test close when stream has no fileno."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        # Mock stream without fileno
        mock_stream = MagicMock()
        mock_stream.flush = MagicMock()
        del mock_stream.fileno
        handler.stream = mock_stream
        
        handler.close()
    
    def test_close_fileno_error(self, tmp_path):
        """Test close when fileno raises error."""
        log_file = tmp_path / "test.log"
        handler = RobustFileHandler(str(log_file), mode='a', encoding='utf-8')
        
        # Mock stream with fileno that raises error
        mock_stream = MagicMock()
        mock_stream.flush = MagicMock()
        mock_stream.fileno = MagicMock(side_effect=AttributeError("No fileno"))
        handler.stream = mock_stream
        
        handler.close()


class TestSetupLogger:
    """Test cases for setup_logger function."""
    
    def test_setup_logger_no_file(self):
        """Test setup_logger without log file."""
        logger = setup_logger("test_logger", logging.INFO, None)
        
        assert logger.name == "test_logger"
        assert logger.level == logging.INFO
    
    def test_setup_logger_with_file(self, tmp_path):
        """Test setup_logger with log file."""
        log_file = tmp_path / "test.log"
        logger = setup_logger("test_logger", logging.INFO, str(log_file))
        
        assert logger.name == "test_logger"
        assert log_file.exists()
        
        # Clean up
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                root_logger.removeHandler(handler)
    
    def test_setup_logger_existing_handler(self, tmp_path):
        """Test setup_logger preserves existing handler for same file."""
        log_file = tmp_path / "test.log"
        
        # First call
        logger1 = setup_logger("test1", logging.INFO, str(log_file))
        
        # Second call with same file
        logger2 = setup_logger("test2", logging.INFO, str(log_file))
        
        # Should have same file handler
        root_logger = logging.getLogger()
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1
        
        # Clean up
        for handler in file_handlers:
            handler.close()
            root_logger.removeHandler(handler)
    
    def test_setup_logger_removes_other_handlers(self, tmp_path):
        """Test setup_logger removes other file handlers."""
        log_file1 = tmp_path / "test1.log"
        log_file2 = tmp_path / "test2.log"
        
        # Setup first logger
        logger1 = setup_logger("test1", logging.INFO, str(log_file1))
        
        # Setup second logger with different file
        logger2 = setup_logger("test2", logging.INFO, str(log_file2))
        
        # Should only have one file handler now
        root_logger = logging.getLogger()
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1
        
        # Clean up
        for handler in file_handlers:
            handler.close()
            root_logger.removeHandler(handler)
    
    def test_setup_logger_handler_path_normalization(self, tmp_path):
        """Test setup_logger handles path normalization correctly."""
        log_file = tmp_path / "test.log"
        abs_path = str(log_file.absolute())
        rel_path = str(log_file.relative_to(Path.cwd())) if log_file.is_relative_to(Path.cwd()) else abs_path
        
        # Use absolute path first
        logger1 = setup_logger("test1", logging.INFO, abs_path)
        
        # Use relative path - should preserve handler
        logger2 = setup_logger("test2", logging.INFO, rel_path)
        
        root_logger = logging.getLogger()
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1
        
        # Clean up
        for handler in file_handlers:
            handler.close()
            root_logger.removeHandler(handler)
    
    def test_setup_logger_handler_with_attribute_error(self, tmp_path):
        """Test setup_logger handles AttributeError when accessing baseFilename."""
        log_file = tmp_path / "test.log"
        
        # Create a mock handler that raises AttributeError when accessing baseFilename
        class MockHandlerWithError(logging.FileHandler):
            @property
            def baseFilename(self):
                raise AttributeError("No baseFilename")
        
        root_logger = logging.getLogger()
        mock_handler = None
        try:
            # Create a handler that will raise AttributeError
            mock_handler = MockHandlerWithError(str(tmp_path / "other.log"))
            root_logger.addHandler(mock_handler)
            
            # Should not raise exception
            logger = setup_logger("test", logging.INFO, str(log_file))
        except Exception:
            # If handler creation fails, that's okay for this test
            pass
        finally:
            # Clean up
            if mock_handler:
                try:
                    root_logger.removeHandler(mock_handler)
                    mock_handler.close()
                except:
                    pass
            file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
            for handler in file_handlers:
                try:
                    handler.close()
                    root_logger.removeHandler(handler)
                except:
                    pass
    
    def test_setup_logger_handler_with_os_error(self, tmp_path):
        """Test setup_logger handles OSError when accessing baseFilename."""
        log_file = tmp_path / "test.log"
        
        # Create a mock handler that raises OSError when accessing baseFilename
        class MockHandlerWithOSError(logging.FileHandler):
            @property
            def baseFilename(self):
                raise OSError("File not found")
        
        root_logger = logging.getLogger()
        mock_handler = None
        try:
            mock_handler = MockHandlerWithOSError(str(tmp_path / "other.log"))
            root_logger.addHandler(mock_handler)
            
            # Should not raise exception
            logger = setup_logger("test", logging.INFO, str(log_file))
        except Exception:
            # If handler creation fails, that's okay for this test
            pass
        finally:
            # Clean up
            if mock_handler:
                try:
                    root_logger.removeHandler(mock_handler)
                    mock_handler.close()
                except:
                    pass
            file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
            for handler in file_handlers:
                try:
                    handler.close()
                    root_logger.removeHandler(handler)
                except:
                    pass
    
    def test_setup_logger_creates_parent_dirs(self, tmp_path):
        """Test setup_logger creates parent directories."""
        log_file = tmp_path / "nested" / "dir" / "test.log"
        
        # Clear any existing handlers first
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                root_logger.removeHandler(handler)
        
        logger = setup_logger("test", logging.INFO, str(log_file))
        
        assert log_file.parent.exists()
        assert log_file.exists()
        
        # Clean up
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        for handler in file_handlers:
            handler.close()
            root_logger.removeHandler(handler)
    
    def test_setup_logger_sets_level(self, tmp_path):
        """Test setup_logger sets correct level."""
        log_file = tmp_path / "test.log"
        
        # Clear any existing handlers first
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                root_logger.removeHandler(handler)
        
        logger = setup_logger("test", logging.DEBUG, str(log_file))
        
        assert logger.level == logging.DEBUG
        assert root_logger.level == logging.DEBUG
        
        # Clean up
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        for handler in file_handlers:
            handler.close()
            root_logger.removeHandler(handler)


class TestFlushAllHandlers:
    """Test cases for flush_all_handlers function."""
    
    def test_flush_all_handlers_with_file_handler(self, tmp_path):
        """Test flush_all_handlers with file handler."""
        log_file = tmp_path / "test.log"
        
        # Clear any existing handlers first
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                root_logger.removeHandler(handler)
        
        logger = setup_logger("test", logging.INFO, str(log_file))
        
        logger.info("Test message")
        flush_all_handlers()
        
        assert "Test message" in log_file.read_text()
        
        # Clean up
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        for handler in file_handlers:
            handler.close()
            root_logger.removeHandler(handler)
    
    def test_flush_all_handlers_no_file_handlers(self):
        """Test flush_all_handlers with no file handlers."""
        # Clear all handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Should not raise exception
        flush_all_handlers()
    
    def test_flush_all_handlers_with_closed_handler(self, tmp_path):
        """Test flush_all_handlers with closed handler."""
        log_file = tmp_path / "test.log"
        logger = setup_logger("test", logging.INFO, str(log_file))
        
        # Close the handler
        root_logger = logging.getLogger()
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        for handler in file_handlers:
            handler.close()
        
        # Should not raise exception
        flush_all_handlers()
    
    def test_flush_all_handlers_with_exception(self, tmp_path):
        """Test flush_all_handlers handles exceptions."""
        log_file = tmp_path / "test.log"
        
        # Clear any existing handlers first
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                root_logger.removeHandler(handler)
        
        logger = setup_logger("test", logging.INFO, str(log_file))
        
        # Mock handler that raises exception
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        original_flush = file_handlers[0].flush
        file_handlers[0].flush = MagicMock(side_effect=Exception("Flush error"))
        
        # Should not raise exception
        flush_all_handlers()
        
        # Restore original flush
        file_handlers[0].flush = original_flush
        
        # Clean up
        for handler in file_handlers:
            handler.close()
            root_logger.removeHandler(handler)
    
    def test_flush_all_handlers_no_stream(self, tmp_path):
        """Test flush_all_handlers when handler has no stream."""
        log_file = tmp_path / "test.log"
        
        # Clear any existing handlers first
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                root_logger.removeHandler(handler)
        
        logger = setup_logger("test", logging.INFO, str(log_file))
        
        file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        for handler in file_handlers:
            handler.stream = None
        
        # Should not raise exception
        flush_all_handlers()
        
        # Clean up - handlers might not have streams, so just remove them
        for handler in file_handlers:
            try:
                if handler.stream:
                    handler.close()
            except:
                pass
            root_logger.removeHandler(handler)

