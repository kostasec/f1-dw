# dags/utils/scraper/result.py
import requests
import pandas as pd
import time
import random
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper


class ResultScraper(BaseScraper):
    """
    Scraper for F1 race results from Ergast API
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
            'result_id', 'driver_ref', 'constructor_ref', 'race_year', 'race_round',
            'driver_number', 'grid', 'position', 'position_text', 'position_order',
            'points', 'number_of_laps', 'race_duration_hours', 'race_duration_milliseconds',
            'fastest_lap', 'rank', 'fastest_lap_minutes', 'fastest_lap_speed', 'status',
            'year', 'source', 'created_at'
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
    
    def scrape_results_year(self, year: int) -> List[Dict]:
        print(f"Fetching race results for {year}...")
        
        data = self._get(f'{year}/results.json?limit=1000')
        races = data['MRData']['RaceTable']['Races']
        
        all_results = []
        for race in races:
            race_year = race['season']
            race_round = race['round']
            
            for result in race.get('Results', []):
                result['race_year'] = race_year
                result['race_round'] = race_round
                all_results.append(result)
        
        print(f"{len(all_results)} results found across {len(races)} races")
        return all_results
    
    def scrape_results_range(self, year_start: int, year_end: int) -> pd.DataFrame:
        print(f"\n{'='*70}")
        print(f"SCRAPING RACE RESULTS FROM ERGAST API: {year_start}-{year_end}")
        print(f"{'='*70}\n")
        
        all_results = []
        
        for year in range(year_start, year_end + 1):
            results = self.scrape_results_year(year)
            all_results.extend(results)
        
        df = pd.DataFrame(all_results)
        
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
        
        # Extract nested Driver, Constructor data
        df_api['driver_ref'] = df_api['Driver'].apply(
            lambda x: x.get('driverId') if isinstance(x, dict) else None
        )
        df_api['constructor_ref'] = df_api['Constructor'].apply(
            lambda x: x.get('constructorId') if isinstance(x, dict) else None
        )
        
        # Extract nested Time data
        df_api['race_duration_hours'] = df_api['Time'].apply(
            lambda x: x.get('time') if isinstance(x, dict) else None
        )
        df_api['race_duration_milliseconds'] = df_api['Time'].apply(
            lambda x: x.get('millis') if isinstance(x, dict) else None
        )
        
        # Extract nested FastestLap data
        def extract_fastest_lap(row, field):
            fastest_lap = row.get('FastestLap')
            
            if not isinstance(fastest_lap, dict):
                return None
            
            if field in ('rank', 'lap'):
                return fastest_lap.get(field)
            elif field == 'time':
                return fastest_lap.get('Time', {}).get('time')
            elif field == 'speed':
                return fastest_lap.get('AverageSpeed', {}).get('speed')
            
            return None
        
        df_api['rank'] = df_api.apply(lambda x: extract_fastest_lap(x, 'rank'), axis=1)
        df_api['fastest_lap'] = df_api.apply(lambda x: extract_fastest_lap(x, 'lap'), axis=1)
        df_api['fastest_lap_minutes'] = df_api.apply(lambda x: extract_fastest_lap(x, 'time'), axis=1)
        df_api['fastest_lap_speed'] = df_api.apply(lambda x: extract_fastest_lap(x, 'speed'), axis=1)
        
        # Sort by year + round + position
        df_api['positionOrder_sort'] = pd.to_numeric(df_api.get('positionOrder', df_api.get('position')), errors='coerce').fillna(999)
        df_api = df_api.sort_values(['race_year', 'race_round', 'positionOrder_sort']).reset_index(drop=True)
        
        print(f"Sorted by year + round + position for stable result_id generation")
        
        # Generate result_id
        df_api['result_id'] = range(10000001, 10000001 + len(df_api))
        
        print(f"Generated result_id: {df_api['result_id'].min()} - {df_api['result_id'].max()}")
        
        # Map to Silver schema
        df_transformed = pd.DataFrame({
            'result_id': df_api['result_id'],
            'driver_ref': df_api['driver_ref'],
            'constructor_ref': df_api['constructor_ref'],
            'race_year': df_api['race_year'],
            'race_round': df_api['race_round'],
            'driver_number': df_api.get('number'),
            'grid': df_api.get('grid'),
            'position': df_api.get('position'),
            'position_text': df_api.get('positionText'),
            'position_order': df_api.get('positionOrder', df_api.get('position')),
            'points': df_api.get('points'),
            'number_of_laps': df_api.get('laps'),
            'race_duration_hours': df_api['race_duration_hours'],
            'race_duration_milliseconds': df_api['race_duration_milliseconds'],
            'fastest_lap': df_api['fastest_lap'],
            'rank': df_api['rank'],
            'fastest_lap_minutes': df_api['fastest_lap_minutes'],
            'fastest_lap_speed': df_api['fastest_lap_speed'],
            'status': df_api.get('status')
        })
        
        # Type conversions
        df_transformed['result_id'] = df_transformed['result_id'].astype('Int64')
        df_transformed['race_year'] = pd.to_numeric(df_transformed['race_year'], errors='coerce').astype('Int64')
        df_transformed['race_round'] = pd.to_numeric(df_transformed['race_round'], errors='coerce').astype('Int64')
        df_transformed['driver_number'] = pd.to_numeric(df_transformed['driver_number'], errors='coerce').fillna(0).astype('Int64')
        df_transformed['grid'] = pd.to_numeric(df_transformed['grid'], errors='coerce').fillna(0).astype('Int64')
        df_transformed['position_order'] = pd.to_numeric(df_transformed['position_order'], errors='coerce').fillna(0).astype('Int64')
        df_transformed['number_of_laps'] = pd.to_numeric(df_transformed['number_of_laps'], errors='coerce').fillna(0).astype('Int64')
        df_transformed['fastest_lap'] = pd.to_numeric(df_transformed['fastest_lap'], errors='coerce')
        df_transformed['rank'] = pd.to_numeric(df_transformed['rank'], errors='coerce')
        
        df_transformed['position'] = pd.to_numeric(df_transformed['position'], errors='coerce')
        df_transformed['points'] = pd.to_numeric(df_transformed['points'], errors='coerce')
        df_transformed['race_duration_milliseconds'] = pd.to_numeric(df_transformed['race_duration_milliseconds'], errors='coerce')
        df_transformed['fastest_lap_speed'] = pd.to_numeric(df_transformed['fastest_lap_speed'], errors='coerce')
        
        df_transformed['position_text'] = df_transformed['position_text'].astype(str)
        df_transformed['status'] = df_transformed['status'].astype(str)
        
        # Metadata columns
        df_transformed['year'] = df_transformed['race_year']
        df_transformed['source'] = 'api'
        df_transformed['created_at'] = datetime.now()
        
        print(f"Transformed {len(df_transformed)} results to Silver schema")
        print(f"Source: api")
        print(f"result_id range: {df_transformed['result_id'].min()} - {df_transformed['result_id'].max()}")
        
        return df_transformed
