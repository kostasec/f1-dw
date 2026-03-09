# dags/utils/scraper/lap.py

import requests
import pandas as pd
import time
import random
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper


class LapScraper(BaseScraper):
    """
    Scraper for F1 lap times from Ergast API
    """
    
    BASE_URL = 'https://api.jolpi.ca/ergast/f1'
    RATE_LIMIT_DELAY = 20.0
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
            'driver_ref', 'race_year', 'race_round', 'lap', 'lap_position',
            'lap_minutes', 'lap_milliseconds', 'year', 'source', 'created_at'
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
    
    def _get_rounds_for_year(self, year: int) -> int:
        """Get number of rounds in a season"""
        data = self._get(f'{year}.json')
        races = data['MRData']['RaceTable']['Races']
        return len(races)
    
    def scrape_laps_race(self, year: int, round_num: int) -> List[Dict]:
        print(f"  Fetching laps for {year} Round {round_num}...")
        
        data = self._get(f'{year}/{round_num}/laps.json')
        races = data['MRData']['RaceTable']['Races']
        
        if not races:
            print(f"  No data for {year} Round {round_num}")
            return []
        
        race = races[0]
        race_year = race['season']
        race_round = race['round']
        
        all_laps = []
        for lap_data in race.get('Laps', []):
            lap_number = lap_data['number']
            for timing in lap_data.get('Timings', []):
                all_laps.append({
                    'race_year': race_year,
                    'race_round': race_round,
                    'lap': lap_number,
                    'driver_ref': timing['driverId'],
                    'lap_position': timing['position'],
                    'lap_minutes': timing['time']
                })
        
        print(f"    {len(all_laps)} lap times found")
        return all_laps
    
    def scrape_laps_year(self, year: int) -> List[Dict]:
        print(f"\nScraping laps for {year}...")
        
        num_rounds = self._get_rounds_for_year(year)
        print(f"  Found {num_rounds} rounds in {year}")
        
        all_laps = []
        for round_num in range(1, num_rounds + 1):
            laps = self.scrape_laps_race(year, round_num)
            all_laps.extend(laps)
        
        print(f"Total laps for {year}: {len(all_laps):,}")
        return all_laps
    
    def scrape_laps_range(self, year_start: int, year_end: int) -> pd.DataFrame:
        print(f"\n{'='*70}")
        print(f"SCRAPING LAP TIMES FROM ERGAST API: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        all_laps = []
        
        for year in range(year_start, year_end + 1):
            laps = self.scrape_laps_year(year)
            all_laps.extend(laps)
        
        df = pd.DataFrame(all_laps)
        
        print(f"\n{'='*70}")
        print(f"SCRAPING COMPLETE")
        print(f"Total records: {len(df):,}")
        print(f"Years covered: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        return df
    
    def transform_to_silver_schema(self, df_api: pd.DataFrame) -> list:
        """
        Transform raw API data to Silver layer schema
        
        Args:
            df_api: Raw API DataFrame
        
        Returns:
            List of dict records (for Kafka compatibility)
        """
        print("Transforming API data to Silver schema...")
        
        #  EMPTY CHECK - Return empty list (not DataFrame)
        if df_api.empty:
            print(f"⚠️  No data found - returning empty list")
            return []
        
        # Sort by year + round + lap + position
        df_api['lap_sort'] = pd.to_numeric(df_api['lap'], errors='coerce')
        df_api['position_sort'] = pd.to_numeric(df_api['lap_position'], errors='coerce')
        df_api = df_api.sort_values(['race_year', 'race_round', 'lap_sort', 'position_sort']).reset_index(drop=True)
        
        print(f"Sorted by year + round + lap + position")
        
        # Convert lap_minutes to milliseconds
        def lap_time_to_milliseconds(time_str):
            if pd.isna(time_str) or time_str == '':
                return None
            
            try:
                parts = str(time_str).split(':')
                if len(parts) == 2:
                    minutes = int(parts[0])
                    seconds = float(parts[1])
                    return int((minutes * 60 + seconds) * 1000)
                else:
                    return int(float(time_str) * 1000)
            except:
                return None
        
        df_api['lap_milliseconds'] = df_api['lap_minutes'].apply(lap_time_to_milliseconds)
        
        # Map to Silver schema
        df_transformed = pd.DataFrame({
            'driver_ref': df_api['driver_ref'],
            'race_year': df_api['race_year'],
            'race_round': df_api['race_round'],
            'lap': df_api['lap'],
            'lap_position': df_api['lap_position'],
            'lap_minutes': df_api['lap_minutes'],
            'lap_milliseconds': df_api['lap_milliseconds']
        })
        
        # Type conversions
        df_transformed['race_year'] = pd.to_numeric(df_transformed['race_year'], errors='coerce').astype('Int64')
        df_transformed['race_round'] = pd.to_numeric(df_transformed['race_round'], errors='coerce').astype('Int64')
        df_transformed['lap'] = pd.to_numeric(df_transformed['lap'], errors='coerce').astype('Int64')
        df_transformed['lap_position'] = pd.to_numeric(df_transformed['lap_position'], errors='coerce').astype('Int64')
        df_transformed['lap_milliseconds'] = pd.to_numeric(df_transformed['lap_milliseconds'], errors='coerce').astype('Int64')
        
        df_transformed['driver_ref'] = df_transformed['driver_ref'].astype(str)
        df_transformed['lap_minutes'] = df_transformed['lap_minutes'].astype(str)
        
        # Metadata columns
        df_transformed['year'] = df_transformed['race_year']
        df_transformed['source'] = 'api'
        
        current_time = datetime.now()
        df_transformed['created_at'] = current_time
        
        print(f"Transformed {len(df_transformed):,} lap times to Silver schema")
        print(f"Source: api")
        
        #  ALWAYS return list of dicts (convert Pandas Timestamp → Python datetime)
        records = df_transformed.to_dict('records')
        for record in records:
            if isinstance(record['created_at'], pd.Timestamp):
                record['created_at'] = record['created_at'].to_pydatetime()
        
        return records
