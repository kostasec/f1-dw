# dags/utils/scraper/constructorStanding.py

import requests
import pandas as pd
import time
import random
from datetime import datetime
from typing import List, Dict
from .base_scraper import BaseScraper


class ConstructorStandingScraper(BaseScraper):
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
            'cs_id', 'constructor_ref', 'race_year', 'race_round',
            'cs_position', 'cs_points', 'cs_wins', 'year', 'source', 'created_at'
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
    
    def scrape_cstandings_year(self, year: int) -> List[Dict]:
        """Scrape constructor standings for a single year"""
        print(f"Fetching constructor standings for {year}...")

        data = self._get(f'{year}/constructorstandings.json?limit=1000')
        standings_lists = data['MRData']['StandingsTable']['StandingsLists']

        all_standings = []

        for standing in standings_lists:
            race_year = standing['season']
            race_round = standing['round']

            for constructor_standing in standing.get('ConstructorStandings', []):
                record = {
                    'race_year': race_year,
                    'race_round': race_round,
                    'position': constructor_standing.get('position'),
                    'points': constructor_standing.get('points'),
                    'wins': constructor_standing.get('wins'),
                    'constructor_ref': constructor_standing.get('Constructor', {}).get('constructorId')
                }
                all_standings.append(record)
        
        print(f"{len(all_standings)} standings found for {year}")
        return all_standings

    def scrape_cstandings_range(self, year_start: int, year_end: int) -> pd.DataFrame:
        """Scrape constructor standings for a range of years"""
        
        print(f"\n{'='*70}")
        print(f"SCRAPING CONSTRUCTOR STANDINGS FROM ERGAST API: {year_start}-{year_end}")
        print(f"{'='*70}\n")

        all_standings = []

        for year in range(year_start, year_end + 1):
            cstandings = self.scrape_cstandings_year(year)
            all_standings.extend(cstandings)

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

        # Sort by year + round + position
        df_api = df_api.sort_values(
            ['race_year', 'race_round', 'position'],
            ascending=[True, True, True]
        ).reset_index(drop=True)

        print(f"Sorted by year + round + position")

        # Generate cs_id
        df_api['cs_id'] = range(10000001, 10000001 + len(df_api))
        
        print(f"Generated cs_id: {df_api['cs_id'].min()} - {df_api['cs_id'].max()}")

        # Create transformed DataFrame
        df_transformed = pd.DataFrame({
            'cs_id': df_api['cs_id'],
            'constructor_ref': df_api['constructor_ref'],
            'race_year': df_api['race_year'],
            'race_round': df_api['race_round'],
            'cs_position': df_api['position'],
            'cs_points': df_api['points'],
            'cs_wins': df_api['wins']
        })

        # Type conversions
        df_transformed['cs_id'] = df_transformed['cs_id'].astype('Int64')
        df_transformed['race_year'] = pd.to_numeric(df_transformed['race_year'], errors='coerce').astype('Int64')
        df_transformed['race_round'] = pd.to_numeric(df_transformed['race_round'], errors='coerce').astype('Int64')
        df_transformed['cs_position'] = pd.to_numeric(df_transformed['cs_position'], errors='coerce').astype('Int64')
        df_transformed['cs_points'] = pd.to_numeric(df_transformed['cs_points'], errors='coerce')
        df_transformed['cs_wins'] = pd.to_numeric(df_transformed['cs_wins'], errors='coerce').astype('Int64')

        # Metadata
        df_transformed['year'] = df_transformed['race_year']
        df_transformed['source'] = 'api'
        df_transformed['created_at'] = datetime.now()
        
        print(f"Transformed {len(df_transformed)} standings to Silver schema")
        print(f"Source: api")
        print(f"cs_id range: {df_transformed['cs_id'].min()} - {df_transformed['cs_id'].max()}")

        return df_transformed
