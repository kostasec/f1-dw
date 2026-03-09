# dags/utils/scraper/driverStanding.py

import requests
import pandas as pd
import time
import random
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper


class DriverStandingScraper(BaseScraper):
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
            'ds_id', 'driver_ref', 'constructor_ref', 'race_year', 'race_round',
            'ds_position', 'ds_points', 'ds_wins', 'year', 'source', 'created_at'
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
    
    def scrape_dstandings_year(self, year: int) -> List[Dict]:
        """Scrape driver standings for a single year"""
        print(f"Fetching driver standings for {year}...")

        data = self._get(f'{year}/driverstandings.json?limit=1000')
        standings_list = data['MRData']['StandingsTable']['StandingsLists']
        
        all_standings = []
        
        for standing in standings_list:
            race_year = standing['season']
            race_round = standing['round']

            for driver_standing in standing.get('DriverStandings', []):
                record = {
                    'race_year': race_year,
                    'race_round': race_round,
                    'position': driver_standing.get('position'),
                    'points': driver_standing.get('points'),
                    'wins': driver_standing.get('wins'),
                    'driver_ref': driver_standing.get('Driver', {}).get('driverId'),
                    'constructor_ref': driver_standing.get('Constructors', [{}])[0].get('constructorId') if driver_standing.get('Constructors') else None,
                }
                all_standings.append(record)

        print(f"{len(all_standings)} standings found for {year}")
        return all_standings
    
    def scrape_dstandings_range(self, year_start: int, year_end: int) -> pd.DataFrame:
        """Scrape driver standings for a range of years"""
        
        print(f"\n{'='*70}")
        print(f"SCRAPING DRIVER STANDINGS FROM ERGAST API: {year_start}-{year_end}")
        print(f"{'='*70}\n")

        all_standings = []

        for year in range(year_start, year_end + 1):
            dstandings = self.scrape_dstandings_year(year)
            all_standings.extend(dstandings)

        df = pd.DataFrame(all_standings)

        print(f"\n{'='*70}")
        print(f"SCRAPING COMPLETE")
        print(f"Total records: {len(df):,}")
        print(f"Years covered: {year_start}-{year_end}")
        if not df.empty:
            print(f"Unique races: {df[['race_year', 'race_round']].drop_duplicates().shape[0]}")
        print(f"{'='*70}\n")

        return df
    
    def transform_to_silver_schema(self, df_api: pd.DataFrame) -> pd.DataFrame:
        """Transform raw API data to Silver layer schema"""
        
        print("Transforming API data to Silver schema...")

        #  EMPTY CHECK
        if df_api.empty:
            return self.ensure_schema(df_api, self.SCHEMA_COLUMNS)
        
        # Sort
        df_api = df_api.sort_values(
            ['race_year', 'race_round', 'position'], 
            ascending=[True, True, True]
        ).reset_index(drop=True)
        
        print(f"Sorted by year + round + position")
        
        # Generate ds_id
        df_api['ds_id'] = range(10000001, 10000001 + len(df_api))
        
        print(f"Generated ds_id: {df_api['ds_id'].min()} - {df_api['ds_id'].max()}")

        # Create transformed DataFrame
        df_transformed = pd.DataFrame({
            'ds_id': df_api['ds_id'],
            'driver_ref': df_api['driver_ref'],
            'constructor_ref': df_api['constructor_ref'],
            'race_year': df_api['race_year'],
            'race_round': df_api['race_round'],
            'ds_position': df_api['position'],
            'ds_points': df_api['points'],
            'ds_wins': df_api['wins']
        })

        # Type conversions
        df_transformed['ds_id'] = df_transformed['ds_id'].astype('Int64')
        df_transformed['race_year'] = pd.to_numeric(df_transformed['race_year'], errors='coerce').astype('Int64')
        df_transformed['race_round'] = pd.to_numeric(df_transformed['race_round'], errors='coerce').astype('Int64')
        df_transformed['ds_position'] = pd.to_numeric(df_transformed['ds_position'], errors='coerce').astype('Int64')
        df_transformed['ds_points'] = pd.to_numeric(df_transformed['ds_points'], errors='coerce')
        df_transformed['ds_wins'] = pd.to_numeric(df_transformed['ds_wins'], errors='coerce').astype('Int64')

        # Metadata
        df_transformed['year'] = df_transformed['race_year']
        df_transformed['source'] = 'api'
        df_transformed['created_at'] = datetime.now()
        
        print(f"Transformed {len(df_transformed)} standings to Silver schema")
        print(f"Source: api")
        print(f"ds_id range: {df_transformed['ds_id'].min()} - {df_transformed['ds_id'].max()}")

        return df_transformed
