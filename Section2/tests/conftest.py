"""Pytest configuration and fixtures."""

import pytest
import logging
from pyspark.sql import SparkSession
from src.utils.spark_session import create_spark_session, stop_spark_session


_original_handleError = logging.Handler.handleError

def _suppress_py4j_cleanup_error(self, record):
    """Custom handleError that suppresses Py4J cleanup errors."""
    import sys
    import traceback
    
    try:
        # Get the current exception info (set when emit() raised the exception)
        exc_info = sys.exc_info()
        if exc_info[0] is not None:
            exc_type, exc_value, exc_traceback = exc_info
            # Check if this is the "I/O operation on closed file" error
            if exc_type == ValueError and 'closed file' in str(exc_value).lower():
                # Check if it's from Py4J by examining the traceback
                tb_str = ''.join(traceback.format_exception(*exc_info))
                if 'py4j' in tb_str.lower() or 'clientserver' in tb_str.lower():
                    # This is the Py4J cleanup error, silently suppress it
                    return
        
        if hasattr(record, 'name') and record.name == 'py4j.clientserver':
            return
    except Exception:
        pass
    
    _original_handleError(self, record)

# Apply the monkey-patch
logging.Handler.handleError = _suppress_py4j_cleanup_error


@pytest.fixture(scope="function")
def spark_session(request):
    """Create a Spark session for testing."""
    import time
    # Use a unique app name per test to avoid conflicts
    test_name = request.node.name if hasattr(request, 'node') else f"TestSession_{int(time.time() * 1000000)}"
    spark = create_spark_session(test_name, "local[1]")
    try:
        test_df = spark.createDataFrame([(1, "test")], ["id", "value"])
        test_df.collect()  # Force evaluation to ensure Java backend is active
        
        from pyspark.sql.functions import udf
        from pyspark.sql.types import IntegerType
        test_udf = udf(lambda x: x, IntegerType())
        _ = test_udf(test_df["id"])  # This will fail if SparkContext isn't ready
    except Exception as e:
        import warnings
        warnings.warn(f"Spark session initialization warning: {e}")
    
    yield spark
    import logging
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            try:
                handler.flush()
                if hasattr(handler, 'stream') and handler.stream:
                    handler.stream.flush()
            except Exception:
                pass
    
    # Now stop Spark session
    stop_spark_session(spark)


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing."""
    data = [
        ("001", "John", "Doe", "2024-01-15", "APPROVED", "2024-01-10"),
        ("002", "Jane", "Smith", "2024-02-20", "REJECTED", "2024-02-18"),
        ("003", None, "Brown", None, "PENDING", None),
    ]
    columns = ["application_id", "first_name", "last_name", "decision_date", "status", "submission_date"]
    return spark_session.createDataFrame(data, columns)

