from datetime import datetime
from typing import Optional, Tuple
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


def parse_date_flexible(date_str: Optional[str]) -> Optional[datetime]:
    if not isinstance(date_str, str):
        return None
    
    if not date_str or date_str.strip() == "":
        return None
    
    date_str = date_str.strip()
    
    placeholder_values = ["NOT_A_DATE", "INVALID_DATE", "NULL", "N/A", "NA"]
    if date_str.upper() in placeholder_values:
        return None
    
    formats = [
        "%Y-%m-%d",           # ISO 8601
        "%d/%m/%Y",           # DD/MM/YYYY
        "%m-%d-%Y",           # MM-DD-YYYY
        "%Y-%m-%dT%H:%M:%S",  # ISO timestamp
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO timestamp with microseconds
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except (ValueError, TypeError):
            continue
    
    return None


def is_valid_iso_date(date_str: Optional[str]) -> bool:
    # Handle datetime.date objects (from Spark schema inference)
    if hasattr(date_str, 'strftime'):
        return True
    
    if not date_str or not isinstance(date_str, str):
        return False
    
    date_str = date_str.strip()
    if not date_str:
        return False
    
    # Strict ISO date format: YYYY-MM-DD (exactly 10 characters, with zero-padded months and days)
    if len(date_str) != 10:
        return False
    
    # Check format: YYYY-MM-DD (4 digits, dash, 2 digits, dash, 2 digits)
    if date_str[4] != '-' or date_str[7] != '-':
        return False
    
    # Check that all parts are digits
    parts = date_str.split('-')
    if len(parts) != 3:
        return False
    
    if len(parts[0]) != 4 or len(parts[1]) != 2 or len(parts[2]) != 2:
        return False
    
    if not (parts[0].isdigit() and parts[1].isdigit() and parts[2].isdigit()):
        return False
    
    # Validate that it's a valid date
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def can_canonicalize_to_iso(date_str: Optional[str]) -> bool:
    if hasattr(date_str, 'strftime'):
        return True
    
    parsed = parse_date_flexible(date_str)
    return parsed is not None


def compare_dates(date1_str: Optional[str], date2_str: Optional[str]) -> Optional[bool]:
    date1 = parse_date_flexible(date1_str)
    date2 = parse_date_flexible(date2_str)
    
    if date1 is None or date2 is None:
        return None
    
    return date1 >= date2


def calculate_days_difference(date1_str: Optional[str], date2_str: Optional[str]) -> Optional[int]:
    date1 = parse_date_flexible(date1_str)
    date2 = parse_date_flexible(date2_str)
    
    if date1 is None or date2 is None:
        return None
    
    return (date1 - date2).days



