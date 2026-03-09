import requests
import pandas as pd
import time
import random
from datetime import datetime
from .base_scraper import BaseScraper


class CircuitScraper(BaseScraper):
    BASE_URL = 'https://api.jolpi.ca/ergast/f1'
    RATE_LIMIT_DELAY = 20.0
    API_CIRCUIT_ID_START = 10000
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
            'circuit_references', 'circuit_name', 'circuit_city', 'circuit_country',
            'circuit_latitude', 'circuit_longtitude', 'circuit_altitude', 'circuit_url',
            'circuit_id', 'source', 'created_at'
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

    def scrape_all_circuits(self) -> pd.DataFrame:
        print(f"\n{'='*70}")
        print(f"SCRAPING ALL CIRCUITS FROM API")
        print(f"{'='*70}\n")

        limit = 100
        offset = 0
        all_circuits = []

        while True:
            print(f"Fetching circuits (offset={offset}, limit={limit})...")
            data = self._get(f'circuits.json?limit={limit}&offset={offset}')
            circuits = data['MRData']['CircuitTable']['Circuits']
            total = int(data['MRData']['total'])
            all_circuits.extend(circuits)
            print(f"Fetched {len(circuits)} circuits (total so far: {len(all_circuits)}/{total})")

            if len(all_circuits) >= total:
                break
            offset += limit

        df = pd.DataFrame(all_circuits)

        print(f"\n{'='*70}")
        print(f"SCRAPING COMPLETE")
        print(f"Total records: {len(df):,}")
        print(f"Unique circuits (by circuitId): {df['circuitId'].nunique() if not df.empty else 0}")
        print(f"{'='*70}\n")

        return df
    
    def transform_to_silver_schema(self, df_api: pd.DataFrame) -> pd.DataFrame:
        print("Transforming API data to Silver schema...")

        #  EMPTY CHECK
        if df_api.empty:
            return self.ensure_schema(df_api, self.SCHEMA_COLUMNS)

        # Deduplicate
        df_unique = df_api.drop_duplicates(subset=['circuitId']).copy()
        print(f"Deduplication: {len(df_api):,} records → {len(df_unique)} unique circuits")

        # Sort alphabetically
        df_unique = df_unique.sort_values('circuitId').reset_index(drop=True)
        print(f"Sorted by circuitId for stable circuit_id generation")

        # Extract nested Location fields
        df_unique['locality'] = df_unique['Location'].apply(lambda x: x.get('locality') if isinstance(x, dict) else None)
        df_unique['country'] = df_unique['Location'].apply(lambda x: x.get('country') if isinstance(x, dict) else None)
        df_unique['lat'] = df_unique['Location'].apply(lambda x: x.get('lat') if isinstance(x, dict) else None)
        df_unique['long'] = df_unique['Location'].apply(lambda x: x.get('long') if isinstance(x, dict) else None)
        
        df_transformed = pd.DataFrame({
            'circuit_references': df_unique['circuitId'],
            'circuit_name': df_unique['circuitName'],
            'circuit_city': df_unique['locality'],
            'circuit_country': df_unique['country'],
            'circuit_latitude': df_unique['lat'],
            'circuit_longtitude': df_unique['long'],
            'circuit_altitude': None,  # Not provided by API
            'circuit_url': df_unique['url']
        })

        # Generate circuit_id
        df_transformed['circuit_id'] = range(
            self.API_CIRCUIT_ID_START + 1,
            self.API_CIRCUIT_ID_START + len(df_transformed) + 1
        )
        
        # Type conversions
        df_transformed['circuit_latitude'] = pd.to_numeric(
            df_transformed['circuit_latitude'], 
            errors='coerce'
        )
        df_transformed['circuit_longtitude'] = pd.to_numeric(
            df_transformed['circuit_longtitude'], 
            errors='coerce'
        )
        
        # Metadata columns
        df_transformed['source'] = 'api'
        df_transformed['created_at'] = datetime.now()

        print(f"Transformed {len(df_transformed)} circuits to Silver schema")
        print(f"Source: api")
        print(f"circuit_id range: {self.API_CIRCUIT_ID_START + 1} - {self.API_CIRCUIT_ID_START + len(df_transformed)}")

        return df_transformed
