"""Unit tests for config loader."""

import pytest
import yaml
import tempfile
import os
from pathlib import Path
from src.config.config_loader import load_config, get_default_config


class TestConfigLoader:
    """Test cases for config loader."""
    
    def test_get_default_config(self):
        """Test getting default configuration."""
        config = get_default_config()
        
        assert isinstance(config, dict)
        assert "timeliness" in config
        assert "dataset" in config
        assert config["timeliness"]["sla_days"] == 30
        assert config["timeliness"]["max_age_years"] == 5
        assert config["dataset"]["name"] == "grant_applications"
    
    def test_load_config_default_path(self):
        """Test loading config with default path."""
        # Should load from default location if it exists
        config = load_config()
        
        assert isinstance(config, dict)
        # Should have at least default keys or loaded config keys
        assert len(config) > 0
    
    def test_load_config_custom_path(self):
        """Test loading config from custom path."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_path = f.name
            yaml.dump({
                "timeliness": {"sla_days": 60, "max_age_years": 10},
                "dataset": {"name": "test_dataset"},
                "spark": {"app_name": "TestApp", "master": "local[2]"}
            }, f)
        
        try:
            config = load_config(temp_path)
            
            assert config["timeliness"]["sla_days"] == 60
            assert config["timeliness"]["max_age_years"] == 10
            assert config["dataset"]["name"] == "test_dataset"
            assert config["spark"]["app_name"] == "TestApp"
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_load_config_nonexistent_file(self):
        """Test loading config from nonexistent file returns defaults."""
        nonexistent_path = "/nonexistent/path/config.yaml"
        config = load_config(nonexistent_path)
        
        # Should return default config
        assert config["timeliness"]["sla_days"] == 30
        assert config["dataset"]["name"] == "grant_applications"
    
    def test_load_config_invalid_yaml(self):
        """Test loading config with invalid YAML returns defaults."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_path = f.name
            f.write("invalid: yaml: content: [")
        
        try:
            config = load_config(temp_path)
            # Should return default config on error
            assert config["timeliness"]["sla_days"] == 30
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_load_config_empty_file(self):
        """Test loading config from empty file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_path = f.name
            f.write("")
        
        try:
            config = load_config(temp_path)
            # Should handle empty file gracefully
            assert isinstance(config, dict)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_load_config_none_parameter(self):
        """Test load_config with None parameter uses default path."""
        config = load_config(None)
        assert isinstance(config, dict)
        assert len(config) > 0
    
    def test_load_config_path_object(self):
        """Test load_config accepts Path object."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            temp_path = Path(f.name)
            yaml.dump({"test": "value"}, f)
        
        try:
            config = load_config(temp_path)
            assert config["test"] == "value"
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

