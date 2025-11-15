import yaml
from pathlib import Path
from typing import Dict, Any
from src.utils.logger import setup_logger
import logging

logger = logging.getLogger(__name__)


def load_config(config_path: str = None) -> Dict[str, Any]:
    if config_path is None:
        # Default to config file in src/config directory
        config_path = Path(__file__).parent / "rules_config.yaml"
    
    config_path = Path(config_path)
    
    if not config_path.exists():
        logger.warning(f"Config file not found at {config_path}, using defaults")
        return get_default_config()
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        # Handle empty files - yaml.safe_load returns None for empty files
        if config is None:
            logger.warning(f"Config file is empty at {config_path}, using defaults")
            return get_default_config()
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}, using defaults")
        return get_default_config()


def get_default_config() -> Dict[str, Any]:
    return {
        "timeliness": {
            "sla_days": 30,
            "max_age_years": 5
        },
        "dataset": {
            "name": "grant_applications"
        }
    }

