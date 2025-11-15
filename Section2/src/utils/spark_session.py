import os
import subprocess
from pyspark.sql import SparkSession
from typing import Optional
import logging

logger = logging.getLogger(__name__)


def check_java_installed() -> bool:
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def find_java_home() -> Optional[str]:
    try:
        result = subprocess.run(
            ["/usr/libexec/java_home"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    
    # Try common locations
    common_paths = [
        "/Library/Java/JavaVirtualMachines",
        "/System/Library/Java/JavaVirtualMachines",
        "/usr/lib/jvm",
    ]
    
    for path in common_paths:
        if os.path.exists(path):
            for item in os.listdir(path):
                java_home = os.path.join(path, item, "Contents/Home")
                if os.path.exists(java_home):
                    return java_home
    
    return None


def create_spark_session(
    app_name: str = "DataQualityMonitoring",
    master: str = "local[*]",
    config: Optional[dict] = None
) -> SparkSession:
    # Check for Java
    if not check_java_installed():
        java_home = find_java_home()
        if java_home:
            os.environ["JAVA_HOME"] = java_home
            logger.info(f"Set JAVA_HOME to {java_home}")
        else:
            error_msg = """
Java is required to run PySpark but was not found.

To install Java on macOS:
  1. Install via Homebrew: brew install openjdk@17
  2. Or download from: https://www.oracle.com/java/technologies/downloads/

After installation, set JAVA_HOME:
  export JAVA_HOME=$(/usr/libexec/java_home)
  
Or add to your shell profile (~/.zshrc or ~/.bash_profile):
  export JAVA_HOME=$(/usr/libexec/java_home)
"""
            logger.error(error_msg)
            raise RuntimeError("Java is required but not found. Please install Java to run PySpark.")
    
    builder = SparkSession.builder.appName(app_name).master(master)
    
    # Default configurations
    default_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
    }
    
    # Merge with user-provided config
    if config:
        default_config.update(config)
    
    for key, value in default_config.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()


def stop_spark_session(spark: SparkSession) -> None:
    spark.stop()
    # logger.info("Spark session stopped")

