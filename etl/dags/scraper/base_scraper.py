"""
Base Scraper Class
Provides common functionality for all F1 data scrapers
"""

import pandas as pd
from typing import List, Optional


class BaseScraper:
    """
    Base class for all F1 scrapers
    
    Provides:
    - Empty DataFrame schema handling
    - Common logging utilities
    - Shared validation methods
    """
    
    def ensure_schema(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Ensure DataFrame has correct schema even when empty
        
        Args:
            df: Input DataFrame (potentially empty)
            columns: List of expected column names
            
        Returns:
            DataFrame with correct schema
        """
        if df.empty:
            print(f"No data found - returning empty DataFrame with schema: {columns}")
            return pd.DataFrame(columns=columns)
        return df
    
    def validate_required_columns(self, df: pd.DataFrame, required_cols: List[str]) -> bool:
        """
        Validate that DataFrame contains all required columns
        
        Args:
            df: DataFrame to validate
            required_cols: List of required column names
            
        Returns:
            True if all columns present, False otherwise
        """
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            print(f"Missing required columns: {missing_cols}")
            return False
        return True
    
    def safe_transform(
        self, 
        df_api: pd.DataFrame, 
        transform_func: callable, 
        schema_columns: List[str]
    ) -> pd.DataFrame:
        """
        Safely transform API data with empty DataFrame handling
        
        Args:
            df_api: Raw API DataFrame
            transform_func: Transformation function to apply
            schema_columns: Expected output schema columns
            
        Returns:
            Transformed DataFrame with correct schema
        """
        if df_api.empty:
            return self.ensure_schema(df_api, schema_columns)
        
        try:
            return transform_func(df_api)
        except KeyError as e:
            print(f"KeyError during transformation: {e}")
            print(f"Available columns: {df_api.columns.tolist()}")
            return self.ensure_schema(pd.DataFrame(), schema_columns)