import logging
import sys
import atexit
from pathlib import Path
from typing import Optional
from pythonjsonlogger import json


class RobustFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=False):
        self._filename = str(Path(filename).absolute().resolve())
        self._is_new_file = not Path(filename).exists()
        
        # Always use append mode to prevent truncation if handler is recreated
        # For new files, append mode will create the file
        # For existing files, append mode will preserve content
        super().__init__(filename, mode='a', encoding=encoding, delay=delay)
        
        atexit.register(self._final_flush)
    
    def emit(self, record):
        try:
            super().emit(record)
            if self.stream:
                self.stream.flush()
        except Exception:
            self.handleError(record)
    
    def _final_flush(self):
        try:
            if self.stream:
                self.stream.flush()
                import os
                if hasattr(self.stream, 'fileno'):
                    try:
                        os.fsync(self.stream.fileno())
                    except (OSError, AttributeError):
                        pass
        except Exception:
            pass
    
    def close(self):
        try:
            if self.stream:
                self.stream.flush()
                import os
                if hasattr(self.stream, 'fileno'):
                    try:
                        os.fsync(self.stream.fileno())
                    except (OSError, AttributeError):
                        pass
        except Exception:
            pass
        super().close()


def setup_logger(name: str = __name__, level: int = logging.INFO, log_file: Optional[str] = None) -> logging.Logger:
    # Get root logger to configure all loggers
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    existing_file_handler = None
    if log_file:
        log_file_abs = str(Path(log_file).absolute().resolve())
        for handler in root_logger.handlers:
            if isinstance(handler, logging.FileHandler):
                try:
                    # Normalize both paths for comparison
                    handler_path = str(Path(handler.baseFilename).absolute().resolve())
                    if handler_path == log_file_abs:
                        # Same file - preserve this handler to avoid truncation
                        existing_file_handler = handler
                        break
                except (AttributeError, OSError):
                    continue
    
    # Remove existing handlers to avoid duplicates, but preserve file handler for same file
    handlers_to_remove = []
    for handler in root_logger.handlers:
        if handler is not existing_file_handler:
            handlers_to_remove.append(handler)
    
    for handler in handlers_to_remove:
        root_logger.removeHandler(handler)
        if isinstance(handler, logging.FileHandler):
            handler.flush()
            handler.close()
    
    # Create JSON formatter
    formatter = json.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s'
    )
    
    # Create console handler with JSON formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Create file handler if log_file is provided
    # Only create if we don't already have one for this file (to prevent truncation)
    if log_file and not existing_file_handler:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RobustFileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    elif existing_file_handler:
        # Update formatter on existing handler if needed
        existing_file_handler.setFormatter(formatter)
    
    # Return the specific logger (which will inherit from root)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    return logger


def flush_all_handlers():
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            try:
                handler.flush()
                # Also flush the underlying stream if it exists
                if hasattr(handler, 'stream') and handler.stream:
                    handler.stream.flush()
            except Exception:
                pass

