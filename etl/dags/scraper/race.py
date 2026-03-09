# dags/utils/scraper/race.py

import requests
import pandas as pd
import time
import random
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper


class RaceScraper(BaseScraper):
    """
    Scraper for F1 races from Ergast API
    """
    
    BASE_URL = 'https://api.jolpi.ca/ergast/f1'
    RATE_LIMIT_DELAY = 20.0
    API_RACE_ID_START = 10000
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
            'circuit_ref', 'race_name', 'race_year', 'race_round', 'race_date', 'race_time',
            'fp1_date', 'fp1_time', 'fp2_date', 'fp2_time', 'fp3_date', 'fp3_time',
            'qualification_date', 'qualification_time', 'sprint_date', 'sprint_time',
            'race_url', 'race_id', 'source', 'created_at'
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
    
    def scrape_races_year(self, year: int) -> List[Dict]:
        print(f"Fetching races for {year}...")
        
        data = self._get(f'{year}.json')
        races = data['MRData']['RaceTable']['Races']
        
        print(f"{len(races)} races found")
        return races
    
    def scrape_races_range(self, year_start: int, year_end: int) -> pd.DataFrame:
        print(f"\n{'='*70}")
        print(f"SCRAPING RACES FROM ERGAST API: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        all_races = []
        
        for year in range(year_start, year_end + 1):
            races = self.scrape_races_year(year)
            all_races.extend(races)
        
        df = pd.DataFrame(all_races)
        
        print(f"\n{'='*70}")
        print(f"SCRAPING COMPLETE")
        print(f"Total records: {len(df):,}")
        print(f"Years covered: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        return df
    
    def transform_to_silver_schema(self, df_api: pd.DataFrame) -> pd.DataFrame:
        print("Transforming API data to Silver schema...")
        
        #  EMPTY CHECK
        if df_api.empty:
            return self.ensure_schema(df_api, self.SCHEMA_COLUMNS)
        
        # Extract nested Circuit data
        df_api['circuit_ref'] = df_api['Circuit'].apply(
            lambda x: x.get('circuitId') if isinstance(x, dict) else None
        )
        
        # Extract nested Practice/Qualifying/Sprint data
        def extract_session(row, session_name, field):
            session = row.get(session_name)
            if isinstance(session, dict):
                return session.get(field)
            return None
        
        df_api['fp1_date'] = df_api.apply(lambda x: extract_session(x, 'FirstPractice', 'date'), axis=1)
        df_api['fp1_time'] = df_api.apply(lambda x: extract_session(x, 'FirstPractice', 'time'), axis=1)
        df_api['fp2_date'] = df_api.apply(lambda x: extract_session(x, 'SecondPractice', 'date'), axis=1)
        df_api['fp2_time'] = df_api.apply(lambda x: extract_session(x, 'SecondPractice', 'time'), axis=1)
        df_api['fp3_date'] = df_api.apply(lambda x: extract_session(x, 'ThirdPractice', 'date'), axis=1)
        df_api['fp3_time'] = df_api.apply(lambda x: extract_session(x, 'ThirdPractice', 'time'), axis=1)
        df_api['qualification_date'] = df_api.apply(lambda x: extract_session(x, 'Qualifying', 'date'), axis=1)
        df_api['qualification_time'] = df_api.apply(lambda x: extract_session(x, 'Qualifying', 'time'), axis=1)
        df_api['sprint_date'] = df_api.apply(lambda x: extract_session(x, 'Sprint', 'date'), axis=1)
        df_api['sprint_time'] = df_api.apply(lambda x: extract_session(x, 'Sprint', 'time'), axis=1)
        
        # Sort by year + round
        df_api = df_api.sort_values(['season', 'round']).reset_index(drop=True)
        
        print(f"Sorted by season + round for stable race_id generation")
        
        # Map to Silver schema
        df_transformed = pd.DataFrame({
            'circuit_ref': df_api['circuit_ref'],
            'race_name': df_api['raceName'],
            'race_year': df_api['season'],
            'race_round': df_api['round'],
            'race_date': df_api['date'],
            'race_time': df_api.get('time'),
            'fp1_date': df_api['fp1_date'],
            'fp1_time': df_api['fp1_time'],
            'fp2_date': df_api['fp2_date'],
            'fp2_time': df_api['fp2_time'],
            'fp3_date': df_api['fp3_date'],
            'fp3_time': df_api['fp3_time'],
            'qualification_date': df_api['qualification_date'],
            'qualification_time': df_api['qualification_time'],
            'sprint_date': df_api['sprint_date'],
            'sprint_time': df_api['sprint_time'],
            'race_url': df_api['url']
        })
        
        # Generate race_id
        df_transformed['race_id'] = range(
            self.API_RACE_ID_START + 1,
            self.API_RACE_ID_START + len(df_transformed) + 1
        )
        
        print(f"  Generated race_id: {df_transformed['race_id'].min()} - {df_transformed['race_id'].max()}")
        
        # Type conversions - Dates
        for date_col in ['race_date', 'fp1_date', 'fp2_date', 'fp3_date', 'qualification_date', 'sprint_date']:
            df_transformed[date_col] = pd.to_datetime(
                df_transformed[date_col], 
                errors='coerce'
            ).dt.date
        
        # Time columns
        time_cols = ['race_time', 'fp1_time', 'fp2_time', 'fp3_time', 'qualification_time', 'sprint_time']
        for time_col in time_cols:
            df_transformed[time_col] = pd.to_datetime(df_transformed[time_col], errors='coerce').dt.time
        
        # Integer columns
        df_transformed['race_year'] = pd.to_numeric(df_transformed['race_year'], errors='coerce').astype('Int64')
        df_transformed['race_round'] = pd.to_numeric(df_transformed['race_round'], errors='coerce').astype('Int64')
        
        #  CREATE year_round composite key (for merge and FK lookups)
        df_transformed['year_round'] = (
            df_transformed['race_year'].astype(str) + '_' +
            df_transformed['race_round'].astype(str).str.zfill(2)
        )
        
        # Metadata columns
        df_transformed['source'] = 'api'
        df_transformed['created_at'] = datetime.now()
        
        print(f"Transformed {len(df_transformed)} races to Silver schema")
        print(f"Source: api")
        print(f"race_id range: {self.API_RACE_ID_START + 1} - {self.API_RACE_ID_START + len(df_transformed)}")
        
        return df_transformed
