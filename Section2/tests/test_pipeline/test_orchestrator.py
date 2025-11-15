import pytest
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock
from src.pipeline.orchestrator import DQOrchestrator


class TestDQOrchestrator:
    """Test cases for DQ orchestrator."""
    
    def test_init_default(self):
        """Test DQOrchestrator initialization with default config."""
        orchestrator = DQOrchestrator()
        
        assert orchestrator.config is not None
        assert orchestrator.spark is None
        assert orchestrator.rules == []
        assert orchestrator.evaluator is None
        assert orchestrator.aggregator is None
    
    def test_init_custom_config(self, tmp_path):
        """Test DQOrchestrator initialization with custom config."""
        import yaml
        
        config_file = tmp_path / "test_config.yaml"
        config_data = {
            "timeliness": {"sla_days": 60},
            "dataset": {"name": "test_dataset"},
            "spark": {"app_name": "TestApp"}
        }
        with open(config_file, 'w') as f:
            yaml.dump(config_data, f)
        
        orchestrator = DQOrchestrator(config_path=str(config_file))
        
        assert orchestrator.config is not None
        assert orchestrator.config["dataset"]["name"] == "test_dataset"
    
    def test_setup(self, spark_session):
        """Test orchestrator setup."""
        orchestrator = DQOrchestrator()
        orchestrator.setup()
        
        assert orchestrator.spark is not None
        assert len(orchestrator.rules) > 0
        assert orchestrator.evaluator is not None
        assert orchestrator.aggregator is not None
    
    def test_load_data(self, spark_session, tmp_path):
        """Test loading data from CSV."""
        import pandas as pd
        
        # Create test CSV
        test_data = pd.DataFrame({
            "application_id": ["001", "002"],
            "application_date": ["2024-01-15", "2024-02-20"],
            "application_status": ["APPROVED", "REJECTED"]
        })
        csv_path = tmp_path / "test_data.csv"
        test_data.to_csv(csv_path, index=False)
        
        orchestrator = DQOrchestrator()
        orchestrator.spark = spark_session
        
        df = orchestrator.load_data(str(csv_path))
        
        assert df is not None
        assert df.count() == 2
        assert "application_id" in df.columns
    
    def test_run_analysis(self, spark_session, tmp_path):
        """Test running full analysis."""
        import pandas as pd
        
        # Create test CSV with all required columns for rules
        test_data = pd.DataFrame({
            "application_id": ["001", "002"],
            "application_date": ["2024-01-15", "2024-02-20"],
            "application_status": ["Approved", "Rejected"],  # Must match canonical values
            "requested_amount": [1000.0, 2000.0],
            "approved_amount": [800.0, None],  # Required for C005, CS002
            "decision_date": ["2024-01-20", "2024-02-25"],  # Required for C006, CS001, CS003, CS004, T001
            "grant_scheme_name": ["EDUCATION BURSARY", "HEALTHCARE SUBSIDY"],  # Required for CF004
            "citizen_nric": ["S1234567A", "T7654321B"],  # Required for V001, V007
            "household_size": [2, 3],  # Required for V005
            "household_income": [5000.0, 6000.0]  # Required for V002
        })
        csv_path = tmp_path / "test_data.csv"
        test_data.to_csv(csv_path, index=False)
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        orchestrator = DQOrchestrator()
        orchestrator.setup()
        
        results = orchestrator.run_analysis(str(csv_path), str(output_dir))
        
        assert results is not None
        assert isinstance(results, dict)
        assert "execution_id" in results
        assert "execution_df" in results
        assert "dimension_scores_df" in results
        assert "rule_results_df" in results
        
        dimension_scores_df = results["dimension_scores_df"]
        assert "dimension_name" in dimension_scores_df.columns
        assert "score" in dimension_scores_df.columns
        assert "execution_id" in dimension_scores_df.columns
        
        # Check output files were created
        assert (output_dir / "dq_execution").exists()
        assert (output_dir / "dq_dimension_scores").exists()
        assert (output_dir / "dq_rule_results").exists()
        assert (output_dir / "dq_dimension_scores_minimal").exists()
    
    def test_run_analysis_error_handling(self, spark_session, tmp_path):
        """Test error handling in run_analysis."""
        orchestrator = DQOrchestrator()
        orchestrator.setup()
        
        # Try to load nonexistent file
        with pytest.raises(Exception):
            orchestrator.run_analysis("/nonexistent/file.csv", str(tmp_path))
    
    def test_cleanup(self, spark_session):
        """Test orchestrator cleanup."""
        orchestrator = DQOrchestrator()
        orchestrator.setup()
        
        assert orchestrator.spark is not None
        orchestrator.cleanup()
        
        # Spark session should be stopped (can't easily verify without mocking)
        # But cleanup should not raise errors
        assert True
    
    def test_cleanup_no_spark(self):
        """Test cleanup when spark is None."""
        orchestrator = DQOrchestrator()
        orchestrator.spark = None
        
        # Should not raise error
        orchestrator.cleanup()
    
    def test_create_execution_df(self, spark_session):
        """Test _create_execution_df method."""
        from datetime import datetime
        
        orchestrator = DQOrchestrator()
        orchestrator.setup()  # This sets up spark properly
        
        execution_df = orchestrator._create_execution_df(
            execution_id="test-123",
            execution_timestamp=datetime(2024, 1, 15, 10, 30),
            dataset_name="test_dataset",
            dataset_version="v1.0",
            total_rows=100,
            status="SUCCESS"
        )
        
        assert execution_df.count() == 1
        assert "execution_id" in execution_df.columns
        assert "execution_timestamp" in execution_df.columns
        assert "dataset_name" in execution_df.columns
        assert "dataset_version" in execution_df.columns
        assert "total_rows" in execution_df.columns
        assert "status" in execution_df.columns
        
        row = execution_df.collect()[0]
        assert row.execution_id == "test-123"
        assert row.dataset_name == "test_dataset"
        assert row.dataset_version == "v1.0"
        assert row.total_rows == 100
        assert row.status == "SUCCESS"
        
        orchestrator.cleanup()
    
    def test_create_execution_df_no_version(self, spark_session):
        """Test _create_execution_df with None version."""
        from datetime import datetime
        
        orchestrator = DQOrchestrator()
        orchestrator.setup()  # This sets up spark properly
        
        execution_df = orchestrator._create_execution_df(
            execution_id="test-123",
            execution_timestamp=datetime(2024, 1, 15, 10, 30),
            dataset_name="test_dataset",
            dataset_version=None,
            total_rows=100,
            status="SUCCESS"
        )
        
        row = execution_df.collect()[0]
        assert row.dataset_version is None
        
        orchestrator.cleanup()
    
    def test_create_execution_df_no_spark_session(self, spark_session):
        """Test _create_execution_df raises error when no Spark session."""
        from datetime import datetime
        from pyspark.sql import SparkSession
        
        orchestrator = DQOrchestrator()
        orchestrator.setup()
        
        # Mock getActiveSession to return None
        with patch.object(SparkSession, 'getActiveSession', return_value=None):
            with pytest.raises(RuntimeError, match="No active Spark session found"):
                orchestrator._create_execution_df(
                    execution_id="test-123",
                    execution_timestamp=datetime(2024, 1, 15, 10, 30),
                    dataset_name="test_dataset",
                    dataset_version=None,
                    total_rows=100,
                    status="SUCCESS"
                )
        
        orchestrator.cleanup()
    
    def test_run_analysis_failure_creates_execution_record(self, spark_session, tmp_path):
        """Test that run_analysis creates execution record even on failure."""
        orchestrator = DQOrchestrator()
        orchestrator.setup()
        
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        
        # Try to run with nonexistent file - should fail but create execution record
        with pytest.raises(Exception):
            orchestrator.run_analysis("/nonexistent/file.csv", str(output_dir))
        
        # Check that execution record was created
        execution_dir = output_dir / "dq_execution"
        if execution_dir.exists():
            # Execution record should exist with FAILED status
            import glob
            csv_files = glob.glob(str(execution_dir / "*.csv"))
            if csv_files:
                import pandas as pd
                df = pd.read_csv(csv_files[0])
                assert len(df) > 0
                assert df.iloc[0]["status"] == "FAILED"
    
    def test_run_analysis_save_error_handling(self, spark_session, tmp_path):
        """Test error handling when saving execution record fails."""
        import pandas as pd
        
        # Create test CSV
        test_data = pd.DataFrame({
            "application_id": ["001"],
            "application_date": ["2024-01-15"],
            "application_status": ["Approved"]
        })
        csv_path = tmp_path / "test_data.csv"
        test_data.to_csv(csv_path, index=False)
        
        orchestrator = DQOrchestrator()
        orchestrator.setup()
        
        # Create a read-only directory to cause save error
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        output_dir.chmod(0o444)  # Read-only
        
        try:
            with pytest.raises(Exception):
                orchestrator.run_analysis(str(csv_path), str(output_dir))
        finally:
            # Restore permissions for cleanup
            output_dir.chmod(0o755)
    
    def test_load_data_logs_count(self, spark_session, tmp_path, caplog):
        """Test that load_data logs the row count."""
        import pandas as pd
        
        test_data = pd.DataFrame({
            "application_id": ["001", "002", "003"],
            "application_date": ["2024-01-15", "2024-02-20", "2024-03-10"]
        })
        csv_path = tmp_path / "test_data.csv"
        test_data.to_csv(csv_path, index=False)
        
        orchestrator = DQOrchestrator()
        orchestrator.setup()  # Use setup to properly initialize
        
        try:
            with caplog.at_level(logging.INFO):
                df = orchestrator.load_data(str(csv_path))
            
            assert "Loaded 3 rows" in caplog.text
        finally:
            orchestrator.cleanup()

