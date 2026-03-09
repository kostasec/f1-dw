# dags/utils/scraper/constructor.py

import requests
import pandas as pd
import time
import random
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper


class ConstructorScraper(BaseScraper):
    BASE_URL = 'https://api.jolpi.ca/ergast/f1'
    RATE_LIMIT_DELAY = 20.0
    API_CONSTRUCTOR_ID_START = 10000
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
            'constructor_ref', 'constructor_name', 'constructor_nationality',
            'constructor_url', 'constructor_id', 'source', 'created_at'
        ]

    def _get(self, endpoint: str) -> dict:
        url = f"{self.BASE_URL}/{endpoint}"
        jitter = random.uniform(0, 3)
        time.sleep(jitter)
        
        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                delay_with_jitter = self.RATE_LIMIT_DELAY + random.uniform(0, 2)
                time.sleep(delay_with_jitter)
                return response.json()
            
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    wait_time = self.RETRY_BACKOFF * (attempt + 1) + random.uniform(0, 5)
                    print(f"Rate limit hit! Waiting {int(wait_time)}s before retry {attempt + 1}/{self.MAX_RETRIES}...")
                    time.sleep(wait_time)
                    continue
                raise
            
            except requests.exceptions.RequestException as e:
                print(f"ERROR: API request failed for {url}")
                print(f"Reason: {e}")
                raise
        
        raise requests.exceptions.HTTPError(f"Failed after {self.MAX_RETRIES} retries: {url}")

    def scrape_constructors_year(self, year: int) -> List[Dict]:
        print(f"Fetching constructors for {year}...")
        data = self._get(f'{year}/constructors.json')
        constructors = data['MRData']['ConstructorTable']['Constructors']
        print(f"{len(constructors)} constructors found")
        return constructors
    
    def scrape_constructors_range(self, year_start: int, year_end: int) -> pd.DataFrame:
        print(f"\n{'='*70}")
        print(f"SCRAPING CONSTRUCTORS FROM API: {year_start}-{year_end}")
        print(f"{'='*70}\n")

        all_constructors = []

        for year in range(year_start, year_end + 1):
            constructors = self.scrape_constructors_year(year)
            all_constructors.extend(constructors)
        
        df = pd.DataFrame(all_constructors)

        print(f"\n{'='*70}")
        print(f"SCRAPING COMPLETE")
        print(f"Total records: {len(df):,}")
        print(f"Unique constructors (by constructorId): {df['constructorId'].nunique() if not df.empty else 0}")
        print(f"Years covered: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        return df
    
    def transform_to_silver_schema(self, df_api: pd.DataFrame) -> pd.DataFrame:
        print("Transforming API data to Silver schema...")

        #  EMPTY CHECK
        if df_api.empty:
            return self.ensure_schema(df_api, self.SCHEMA_COLUMNS)

        # Deduplicate
        df_unique = df_api.drop_duplicates(subset=['constructorId']).copy()
        print(f"Deduplication: {len(df_api):,} records → {len(df_unique)} unique constructors")

        # Sort alphabetically
        df_unique = df_unique.sort_values('constructorId').reset_index(drop=True)
        print(f"Sorted by constructorId (alphabetically) for stable constructor_id generation")

        # Map API fields to Silver schema
        df_transformed = pd.DataFrame({
            'constructor_ref': df_unique['constructorId'],
            'constructor_name': df_unique['name'],
            'constructor_nationality': df_unique['nationality'],
            'constructor_url': df_unique['url'],
        })
        
        # Generate constructor_id
        df_transformed['constructor_id'] = range(
            self.API_CONSTRUCTOR_ID_START + 1,
            self.API_CONSTRUCTOR_ID_START + len(df_transformed) + 1
        )

        print(f"Generated constructor_id: {df_transformed['constructor_id'].min()} - {df_transformed['constructor_id'].max()}")

        # Metadata columns
        df_transformed['source'] = 'api'
        df_transformed['created_at'] = datetime.now()
        
        print(f"Transformed {len(df_transformed)} constructors to Silver schema")
        print(f"Source: api")
        print(f"constructor_id range: {self.API_CONSTRUCTOR_ID_START + 1} - {self.API_CONSTRUCTOR_ID_START + len(df_transformed)}")
        
        return df_transformed
