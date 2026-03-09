"""
Ergast API Scraper for F1 Driver Data

API: https://api.jolpi.ca/ergast/f1/{year}/drivers/
Coverage: 2011-2025
Natural Key: driver_ref (driverId from API)

Key Strategy:
- driver_key (INTEGER): Surrogate key (generated in merge, 1, 2, 3...)
- driver_id (INTEGER): Natural key (CSV: 1-857; API: 10001+)
- driver_ref (STRING): Primary natural key ("hamilton", "verstappen")
"""

import requests
import pandas as pd
import time
import random
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper


class DriverScraper(BaseScraper):
    """
    Scraper for F1 drivers from Ergast API
    
    API Field Mappings:
    - driverId → driver_ref (natural key)
    - permanentNumber → driver_number
    - code → driver_code
    - givenName → driver_forename
    - familyName → driver_surname
    - dateOfBirth → driver_dob
    - nationality → driver_nationality
    - url → driver_url
    
    Generated Fields:
    - driver_id: INTEGER (10001, 10002, ...) - generated for API data
    - driver_key: INTEGER (858, 859, ...) - generated in merge step
    - source: 'ergast_api'
    - created_at: TIMESTAMP
    """
    
    BASE_URL = 'https://api.jolpi.ca/ergast/f1'
    RATE_LIMIT_DELAY = 20.0
    API_DRIVER_ID_START = 10000 
    MAX_RETRIES = 6
    RETRY_BACKOFF = 30
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'F1-ETL-Pipeline/1.0',
            'Accept': 'application/json'
        })
        
        # Define schema columns
        self.SCHEMA_COLUMNS = [
            'driver_ref', 'driver_number', 'driver_code', 'driver_forename',
            'driver_surname', 'driver_dob', 'driver_nationality', 'driver_url',
            'driver_id', 'source', 'created_at'
        ]
    
    
    def _get(self, endpoint: str) -> dict:
        """
        Make API request with rate limiting and error handling
        
        Args:
            endpoint: API path (e.g., '2025/drivers.json')
        
        Returns:
            JSON response as dict
        
        Raises:
            requests.exceptions.RequestException on failure
        """
        url = f"{self.BASE_URL}/{endpoint}"
        
        # Add random jitter to avoid strict patterns (0-3 seconds)
        jitter = random.uniform(0, 3)
        time.sleep(jitter)
        
        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                # Rate limiting with random jitter
                delay_with_jitter = self.RATE_LIMIT_DELAY + random.uniform(0, 2)
                time.sleep(delay_with_jitter)
                
                return response.json()
            
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:  # Too Many Requests
                    wait_time = self.RETRY_BACKOFF * (attempt + 1) + random.uniform(0, 5)
                    print(f"Rate limit hit! Waiting {int(wait_time)}s before retry {attempt + 1}/{self.MAX_RETRIES}...")
                    time.sleep(wait_time)
                    continue
                raise
            
            except requests.exceptions.RequestException as e:
                print(f"ERROR: API request failed for {url}")
                print(f"Reason: {e}")
                raise
        
        # If all retries exhausted
        raise requests.exceptions.HTTPError(f"Failed after {self.MAX_RETRIES} retries: {url}")
    
    
    def scrape_drivers_year(self, year: int) -> List[Dict]:
        """
        Scrape drivers for a single year
        
        Args:
            year: F1 season (2011-2025)
        
        Returns:
            List of driver dictionaries (raw API response)
        """
        print(f"Fetching drivers for {year}...")
        
        data = self._get(f'{year}/drivers.json')
        drivers = data['MRData']['DriverTable']['Drivers']
        
        print(f"{len(drivers)} drivers found")
        return drivers
    
    
    def scrape_drivers_range(self, year_start: int, year_end: int) -> pd.DataFrame:
        """
        Scrape drivers for a range of years
        
        Args:
            year_start: Start year (e.g., 2024)
            year_end: End year (e.g., 2025)
        
        Returns:
            DataFrame with all driver records (before deduplication)
        """
        print(f"\n{'='*70}")
        print(f"SCRAPING DRIVERS FROM ERGAST API: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        all_drivers = []
        
        for year in range(year_start, year_end + 1):
            drivers = self.scrape_drivers_year(year)
            all_drivers.extend(drivers)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_drivers)
        
        print(f"\n{'='*70}")
        print(f"SCRAPING COMPLETE")
        print(f"Total records: {len(df):,}")
        print(f"Unique drivers (by driverId): {df['driverId'].nunique() if not df.empty else 0}")
        print(f"Years covered: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        return df
    
    
    def transform_to_silver_schema(self, df_api: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw API data to Silver layer schema
        
        Key Strategy:
        - driver_ref (STRING): PRIMARY natural key (from API driverId)
        - driver_id (INTEGER): Generated (10001, 10002, ...) for API data
        - driver_key (INTEGER): Generated in merge step (not here)
        
        Args:
            df_api: Raw API DataFrame (may contain duplicates)
        
        Returns:
            Transformed DataFrame (Silver schema, deduplicated)
        """
        print("Transforming API data to Silver schema...")
        
        if df_api.empty:
            return self.ensure_schema(df_api, self.SCHEMA_COLUMNS)
        
        # ═══════════════════════════════════════════════════════════
        # STEP 1: Deduplicate (same driver in multiple years)
        # ═══════════════════════════════════════════════════════════
        df_unique = df_api.drop_duplicates(subset=['driverId']).copy()
        
        print(f"Deduplication: {len(df_api):,} records → {len(df_unique)} unique drivers")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 2: Sort alphabetically (stable order for driver_id)
        # ═══════════════════════════════════════════════════════════
        df_unique = df_unique.sort_values('driverId').reset_index(drop=True)
        
        print(f"Sorted by driverId (alphabetically) for stable driver_id generation")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 3: Map API fields to Silver schema
        # ═══════════════════════════════════════════════════════════
        df_transformed = pd.DataFrame({
            # NATURAL KEYS:
            'driver_ref': df_unique['driverId'],  # "hamilton" (PRIMARY NK)
            
            # BUSINESS FIELDS:
            'driver_number': df_unique.get('permanentNumber'),
            'driver_code': df_unique.get('code'),
            'driver_forename': df_unique['givenName'],
            'driver_surname': df_unique['familyName'],
            'driver_dob': df_unique['dateOfBirth'],
            'driver_nationality': df_unique['nationality'],
            'driver_url': df_unique['url'],
        })
        
        # ═══════════════════════════════════════════════════════════
        # STEP 4: Generate driver_id (INTEGER range 10001+)
        # ═══════════════════════════════════════════════════════════
        df_transformed['driver_id'] = range(
            self.API_DRIVER_ID_START + 1,
            self.API_DRIVER_ID_START + len(df_transformed) + 1
        )
        
        print(f"  Generated driver_id: {df_transformed['driver_id'].min()} - {df_transformed['driver_id'].max()}")
        
        # ═══════════════════════════════════════════════════════════
        # STEP 5: Type conversions (match Silver schema)
        # ═══════════════════════════════════════════════════════════
        
        # Date
        df_transformed['driver_dob'] = pd.to_datetime(
            df_transformed['driver_dob'], 
            errors='coerce'
        ).dt.date
        
        # Nullable integer
        df_transformed['driver_number'] = pd.to_numeric(
            df_transformed['driver_number'], 
            errors='coerce'
        ).astype('Int64')
        
        # ═══════════════════════════════════════════════════════════
        # STEP 6: Metadata columns
        # ═══════════════════════════════════════════════════════════
        df_transformed['source'] = 'api'
        df_transformed['created_at'] = datetime.now()
        
        print(f"Transformed {len(df_transformed)} drivers to Silver schema")
        print(f"Source: api")
        print(f"driver_id range: {self.API_DRIVER_ID_START + 1} - {self.API_DRIVER_ID_START + len(df_transformed)}")
        
        return df_transformed
