"""Unit tests for Spark session utility."""

import pytest
import os
import subprocess
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.utils.spark_session import (
    check_java_installed,
    find_java_home,
    create_spark_session,
    stop_spark_session
)


class TestSparkSession:
    """Test cases for Spark session utility."""
    
    def test_check_java_installed_success(self):
        """Test checking Java installation when Java is available."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            assert check_java_installed() is True
            mock_run.assert_called_once()
    
    def test_check_java_installed_not_found(self):
        """Test checking Java when Java is not found."""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = FileNotFoundError()
            assert check_java_installed() is False
    
    def test_check_java_installed_timeout(self):
        """Test checking Java when subprocess times out."""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired('java', 5)
            assert check_java_installed() is False
    
    def test_check_java_installed_nonzero_returncode(self):
        """Test checking Java when command returns non-zero."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            assert check_java_installed() is False
    
    def test_find_java_home_macos_success(self):
        """Test finding JAVA_HOME on macOS via java_home command."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home\n"
            )
            result = find_java_home()
            assert result == "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
    
    def test_find_java_home_macos_not_found(self):
        """Test finding JAVA_HOME when java_home command fails."""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            with patch('os.path.exists', return_value=False):
                result = find_java_home()
                assert result is None
    
    def test_find_java_home_common_paths(self):
        """Test finding JAVA_HOME in common paths."""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = FileNotFoundError()
            with patch('os.path.exists') as mock_exists:
                with patch('os.listdir') as mock_listdir:
                    mock_exists.side_effect = lambda p: p == "/Library/Java/JavaVirtualMachines"
                    mock_listdir.return_value = ["jdk-17.jdk"]
                    mock_exists.side_effect = lambda p: (
                        p == "/Library/Java/JavaVirtualMachines" or
                        p == "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
                    )
                    result = find_java_home()
                    assert result is not None
    
    def test_find_java_home_timeout(self):
        """Test finding JAVA_HOME when subprocess times out."""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired('/usr/libexec/java_home', 5)
            with patch('os.path.exists', return_value=False):
                result = find_java_home()
                assert result is None
    
    def test_create_spark_session_success(self):
        """Test creating Spark session successfully."""
        with patch('src.utils.spark_session.check_java_installed', return_value=True):
            with patch('pyspark.sql.SparkSession.builder') as mock_builder:
                mock_session = MagicMock(spec=SparkSession)
                mock_builder.return_value.appName.return_value.master.return_value.config.return_value.getOrCreate.return_value = mock_session
                
                session = create_spark_session("TestApp", "local[*]")
                assert session is not None
    
    def test_create_spark_session_with_config(self):
        """Test creating Spark session with custom config."""
        with patch('src.utils.spark_session.check_java_installed', return_value=True):
            with patch('pyspark.sql.SparkSession.builder') as mock_builder:
                mock_session = MagicMock(spec=SparkSession)
                mock_builder.return_value.appName.return_value.master.return_value.config.return_value.getOrCreate.return_value = mock_session
                
                custom_config = {"spark.sql.shuffle.partitions": "200"}
                session = create_spark_session("TestApp", "local[*]", custom_config)
                assert session is not None
    
    def test_create_spark_session_java_not_found_with_home(self):
        """Test creating Spark session when Java not found but JAVA_HOME can be set."""
        with patch('src.utils.spark_session.check_java_installed', return_value=False):
            with patch('src.utils.spark_session.find_java_home', return_value="/usr/lib/jvm/java"):
                with patch.dict(os.environ, {}, clear=False):
                    with patch('pyspark.sql.SparkSession.builder') as mock_builder:
                        mock_session = MagicMock(spec=SparkSession)
                        mock_builder.return_value.appName.return_value.master.return_value.config.return_value.getOrCreate.return_value = mock_session
                        
                        session = create_spark_session()
                        assert session is not None
                        assert os.environ.get("JAVA_HOME") == "/usr/lib/jvm/java"
    
    def test_create_spark_session_java_not_found_no_home(self):
        """Test creating Spark session when Java not found and no JAVA_HOME."""
        with patch('src.utils.spark_session.check_java_installed', return_value=False):
            with patch('src.utils.spark_session.find_java_home', return_value=None):
                with pytest.raises(RuntimeError, match="Java is required"):
                    create_spark_session()
    
    def test_stop_spark_session(self):
        """Test stopping Spark session."""
        mock_session = MagicMock(spec=SparkSession)
        stop_spark_session(mock_session)
        mock_session.stop.assert_called_once()
    
    def test_create_spark_session_default_config(self):
        """Test that default Spark config is applied."""
        with patch('src.utils.spark_session.check_java_installed', return_value=True):
            # Create a mock builder that properly chains method calls
            mock_builder_instance = MagicMock()
            # Make all methods return the same instance for chaining
            mock_builder_instance.appName.return_value = mock_builder_instance
            mock_builder_instance.master.return_value = mock_builder_instance
            mock_builder_instance.config.return_value = mock_builder_instance
            mock_builder_instance.getOrCreate.return_value = MagicMock(spec=SparkSession)
            
            # Patch SparkSession.builder where it's accessed in the function
            # We need to patch it in the spark_session module namespace
            with patch('src.utils.spark_session.SparkSession.builder', mock_builder_instance):
                create_spark_session()
                
                # Verify config was called for default settings (3 default configs)
                # The config method should be called 3 times in the loop
                assert mock_builder_instance.config.call_count >= 3, \
                    f"Expected at least 3 config calls, got {mock_builder_instance.config.call_count}"

