import requests
import pandas as pd
import time
import random
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper


class PitStopScraper(BaseScraper):
    """
    Scraper for F1 pit stops from Ergast API
    """
    
    BASE_URL = 'https://api.jolpi.ca/ergast/f1'
    RATE_LIMIT_DELAY = 2
    MAX_RETRIES = 5
    RETRY_BACKOFF = 5
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'F1-ETL-Pipeline/1.0',
            'Accept': 'application/json'
        })
        
        # Define schema columns
        self.SCHEMA_COLUMNS = [
            'driver_ref', 'race_year', 'race_round', 'stop_number', 'lap',
            'pit_time', 'duration_seconds', 'year', 'source', 'created_at'
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
    
    def scrape_pitstops_race(self, year: int, round_num: int) -> List[Dict]:
        print(f"Fetching pit stops for {year} Round {round_num}...")
        
        try:
            data = self._get(f'{year}/{round_num}/pitstops.json?limit=1000')
            races = data['MRData']['RaceTable']['Races']
            
            if not races:
                print(f"  No data for {year} Round {round_num}")
                return []
            
            race = races[0]
            race_year = race['season']
            race_round = race['round']

            all_pitstops = []
            for pitstop_data in race.get('PitStops', []):
                all_pitstops.append({
                    'race_year': race_year,
                    'race_round': race_round,
                    'driver_ref': pitstop_data['driverId'],
                    'stop': pitstop_data['stop'],
                    'lap': pitstop_data['lap'],
                    'pit_time': pitstop_data['time'],
                    'duration_seconds': pitstop_data['duration']
                })
            
            print(f"  {len(all_pitstops)} pit stops found")
            return all_pitstops
        
        except Exception as e:
            print(f"  ERROR fetching {year} Round {round_num}: {e}")
            return []
    
    def scrape_pitstops_year(self, year: int) -> List[Dict]:
        print(f"\n{'='*70}")
        print(f"SCRAPING PIT STOPS FOR {year}")
        print(f"{'='*70}\n")
        
        all_pit_stops = []
        consecutive_empty = 0
        
        for round_num in range(1, 26):
            pit_stops = self.scrape_pitstops_race(year, round_num)
            
            if not pit_stops:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    print(f"  Stopping: 3 consecutive empty rounds detected")
                    break
                continue
            
            consecutive_empty = 0
            all_pit_stops.extend(pit_stops)
        
        print(f"\nTotal pit stops for {year}: {len(all_pit_stops)}")
        return all_pit_stops
    
    def scrape_pitstops_range(self, year_start: int, year_end: int) -> pd.DataFrame:
        print(f"\n{'='*70}")
        print(f"SCRAPING PIT STOPS FROM ERGAST API: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        all_pit_stops = []
        
        for year in range(year_start, year_end + 1):
            pit_stops = self.scrape_pitstops_year(year)
            all_pit_stops.extend(pit_stops)
        
        df = pd.DataFrame(all_pit_stops)
        
        print(f"\n{'='*70}")
        print(f"SCRAPING COMPLETE")
        print(f"Total pit stops: {len(df):,}")
        print(f"Years covered: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        return df
    
    def transform_to_silver_schema(self, df_api: pd.DataFrame) -> pd.DataFrame:
        print("Transforming API data to Silver schema...")
        
        #  EMPTY CHECK
        if df_api.empty:
            return self.ensure_schema(df_api, self.SCHEMA_COLUMNS)
        
        # Sort by year + round + stop + lap
        df_api['stop_sort'] = pd.to_numeric(df_api['stop'], errors='coerce')
        df_api['lap_sort'] = pd.to_numeric(df_api['lap'], errors='coerce')
        df_api = df_api.sort_values(
            ['race_year', 'race_round', 'lap_sort', 'stop_sort']
        ).reset_index(drop=True)

        print(f"Sorted by year + round + lap + stop")
        
        # Map to Silver schema
        df_transformed = pd.DataFrame({
            'driver_ref': df_api['driver_ref'],
            'race_year': df_api['race_year'],
            'race_round': df_api['race_round'],
            'stop_number': df_api['stop'],
            'lap': df_api['lap'],
            'pit_time': df_api['pit_time'],
            'duration_seconds': df_api['duration_seconds']
        })
        
        # Type conversions
        df_transformed['race_year'] = pd.to_numeric(df_transformed['race_year'], errors='coerce').astype('Int64')
        df_transformed['race_round'] = pd.to_numeric(df_transformed['race_round'], errors='coerce').astype('Int64')
        df_transformed['stop_number'] = pd.to_numeric(df_transformed['stop_number'], errors='coerce').astype('Int64')
        df_transformed['lap'] = pd.to_numeric(df_transformed['lap'], errors='coerce').astype('Int64')
        df_transformed['duration_seconds'] = pd.to_numeric(df_transformed['duration_seconds'], errors='coerce')
        df_transformed['driver_ref'] = df_transformed['driver_ref'].astype(str)
        df_transformed['pit_time'] = df_transformed['pit_time'].astype(str)
        
        # Metadata columns
        df_transformed['year'] = df_transformed['race_year']
        df_transformed['source'] = 'api'
        df_transformed['created_at'] = datetime.now()
        
        print(f"Transformed {len(df_transformed)} pit stops to Silver schema")
        print(f"Source: api")
        
        return df_transformed
