"""Unit tests for date parser utility."""

import pytest
from datetime import datetime
from src.utils.date_parser import (
    parse_date_flexible,
    is_valid_iso_date,
    can_canonicalize_to_iso,
    compare_dates,
    calculate_days_difference
)


class TestDateParser:
    """Test cases for date parser."""
    
    def test_parse_date_flexible_iso_format(self):
        """Test parsing ISO format dates."""
        result = parse_date_flexible("2024-01-15")
        assert result == datetime(2024, 1, 15)
    
    def test_parse_date_flexible_dd_mm_yyyy(self):
        """Test parsing DD/MM/YYYY format."""
        result = parse_date_flexible("15/01/2024")
        assert result == datetime(2024, 1, 15)
    
    def test_parse_date_flexible_mm_dd_yyyy(self):
        """Test parsing MM-DD-YYYY format."""
        result = parse_date_flexible("01-15-2024")
        assert result == datetime(2024, 1, 15)
    
    def test_parse_date_flexible_iso_timestamp(self):
        """Test parsing ISO timestamp."""
        result = parse_date_flexible("2024-01-15T10:30:00")
        assert result == datetime(2024, 1, 15, 10, 30, 0)
    
    def test_parse_date_flexible_iso_timestamp_microseconds(self):
        """Test parsing ISO timestamp with microseconds."""
        result = parse_date_flexible("2024-01-15T10:30:00.123456")
        assert result == datetime(2024, 1, 15, 10, 30, 0, 123456)
    
    def test_parse_date_flexible_none(self):
        """Test parsing None returns None."""
        assert parse_date_flexible(None) is None
    
    def test_parse_date_flexible_empty_string(self):
        """Test parsing empty string returns None."""
        assert parse_date_flexible("") is None
        assert parse_date_flexible("   ") is None
    
    def test_parse_date_flexible_placeholder_values(self):
        """Test parsing placeholder values returns None."""
        placeholders = ["NOT_A_DATE", "INVALID_DATE", "NULL", "N/A", "NA"]
        for placeholder in placeholders:
            assert parse_date_flexible(placeholder) is None
            assert parse_date_flexible(placeholder.lower()) is None
    
    def test_parse_date_flexible_invalid_format(self):
        """Test parsing invalid format returns None."""
        assert parse_date_flexible("invalid-date") is None
        assert parse_date_flexible("2024/13/45") is None
        assert parse_date_flexible("not-a-date") is None
    
    def test_parse_date_flexible_whitespace_trimming(self):
        """Test that whitespace is trimmed."""
        result = parse_date_flexible("  2024-01-15  ")
        assert result == datetime(2024, 1, 15)
    
    def test_is_valid_iso_date_valid(self):
        """Test valid ISO date format."""
        assert is_valid_iso_date("2024-01-15") is True
        assert is_valid_iso_date("2024-12-31") is True
        assert is_valid_iso_date("2000-01-01") is True
    
    def test_is_valid_iso_date_invalid_format(self):
        """Test invalid ISO date formats."""
        assert is_valid_iso_date("2024/01/15") is False
        assert is_valid_iso_date("01-15-2024") is False
        assert is_valid_iso_date("2024-1-15") is False
        assert is_valid_iso_date("2024-01-5") is False
    
    def test_is_valid_iso_date_invalid_date(self):
        """Test invalid dates."""
        assert is_valid_iso_date("2024-13-01") is False
        assert is_valid_iso_date("2024-02-30") is False
        assert is_valid_iso_date("2024-01-32") is False
    
    def test_is_valid_iso_date_none_or_empty(self):
        """Test None and empty strings."""
        assert is_valid_iso_date(None) is False
        assert is_valid_iso_date("") is False
        assert is_valid_iso_date("   ") is False
    
    def test_is_valid_iso_date_wrong_length(self):
        """Test dates with wrong length."""
        assert is_valid_iso_date("2024-01-1") is False
        assert is_valid_iso_date("2024-1-15") is False
        assert is_valid_iso_date("2024-01-155") is False
    
    def test_can_canonicalize_to_iso_valid(self):
        """Test valid dates that can be canonicalized."""
        assert can_canonicalize_to_iso("2024-01-15") is True
        assert can_canonicalize_to_iso("15/01/2024") is True
        assert can_canonicalize_to_iso("01-15-2024") is True
        assert can_canonicalize_to_iso("2024-01-15T10:30:00") is True
    
    def test_can_canonicalize_to_iso_invalid(self):
        """Test invalid dates that cannot be canonicalized."""
        assert can_canonicalize_to_iso("invalid") is False
        assert can_canonicalize_to_iso("NOT_A_DATE") is False
        assert can_canonicalize_to_iso(None) is False
        assert can_canonicalize_to_iso("") is False
    
    def test_compare_dates_valid(self):
        """Test comparing valid dates."""
        assert compare_dates("2024-01-15", "2024-01-10") is True
        assert compare_dates("2024-01-15", "2024-01-15") is True
        assert compare_dates("2024-01-10", "2024-01-15") is False
    
    def test_compare_dates_different_formats(self):
        """Test comparing dates in different formats."""
        assert compare_dates("2024-01-15", "15/01/2024") is True  # Same date
        assert compare_dates("01-15-2024", "2024-01-15") is True  # Same date
    
    def test_compare_dates_invalid(self):
        """Test comparing invalid dates returns None."""
        assert compare_dates("invalid", "2024-01-15") is None
        assert compare_dates("2024-01-15", "invalid") is None
        assert compare_dates(None, "2024-01-15") is None
        assert compare_dates("2024-01-15", None) is None
        assert compare_dates(None, None) is None
    
    def test_calculate_days_difference_valid(self):
        """Test calculating days difference for valid dates."""
        assert calculate_days_difference("2024-01-15", "2024-01-10") == 5
        assert calculate_days_difference("2024-01-10", "2024-01-15") == -5
        assert calculate_days_difference("2024-01-15", "2024-01-15") == 0
    
    def test_calculate_days_difference_different_formats(self):
        """Test calculating days difference with different formats."""
        result = calculate_days_difference("2024-01-15", "10/01/2024")
        assert result == 5
    
    def test_calculate_days_difference_invalid(self):
        """Test calculating days difference for invalid dates returns None."""
        assert calculate_days_difference("invalid", "2024-01-15") is None
        assert calculate_days_difference("2024-01-15", "invalid") is None
        assert calculate_days_difference(None, "2024-01-15") is None
        assert calculate_days_difference("2024-01-15", None) is None
    
    def test_calculate_days_difference_large_span(self):
        """Test calculating days difference for large time spans."""
        result = calculate_days_difference("2024-12-31", "2024-01-01")
        assert result == 365  # 2024 is a leap year, so 365 days
    
    def test_parse_date_flexible_type_error_handling(self):
        """Test that TypeError is handled gracefully."""
        # Pass non-string types that might cause TypeError
        assert parse_date_flexible(12345) is None
        assert parse_date_flexible([]) is None
        assert parse_date_flexible({}) is None
    
    def test_is_valid_iso_date_with_date_object(self):
        """Test is_valid_iso_date with date/datetime object."""
        from datetime import date, datetime
        
        # Test with date object
        date_obj = date(2024, 1, 15)
        assert is_valid_iso_date(date_obj) is True
        
        # Test with datetime object
        datetime_obj = datetime(2024, 1, 15, 10, 30)
        assert is_valid_iso_date(datetime_obj) is True
    
    def test_can_canonicalize_to_iso_with_date_object(self):
        """Test can_canonicalize_to_iso with date/datetime object."""
        from datetime import date, datetime
        
        # Test with date object
        date_obj = date(2024, 1, 15)
        assert can_canonicalize_to_iso(date_obj) is True
        
        # Test with datetime object
        datetime_obj = datetime(2024, 1, 15, 10, 30)
        assert can_canonicalize_to_iso(datetime_obj) is True

