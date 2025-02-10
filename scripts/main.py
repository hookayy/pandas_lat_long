import pandas as pd
from geopy.geocoders import Nominatim, ArcGIS
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded
import time
import json
import os
from tqdm import tqdm
import argparse

class GeocodingCache:
    def __init__(self, cache_file='geocoding_cache.json'):
        self.cache_file = cache_file
        self.cache = self._load_cache()

    def _load_cache(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        return {}

    def save_cache(self):
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f)

    def get(self, address):
        return self.cache.get(address)

    def set(self, address, coordinates):
        self.cache[address] = coordinates
        if len(self.cache) % 10 == 0:
            self.save_cache()

def get_coordinates(address, cache, geolocator, backup_geolocator=None):
    """
    Get coordinates with improved rate limiting and fallback options.
    """
    cached_result = cache.get(address)
    if cached_result:
        return cached_result

    if not address.lower().endswith('indonesia'):
        full_address = f"{address}, Indonesia"
    else:
        full_address = address

    for geocoder in [geolocator, backup_geolocator]:
        if geocoder is None:
            continue
            
        try:
            time.sleep(2)
            location = geocoder.geocode(full_address)
            if location:
                result = (location.latitude, location.longitude)
                cache.set(address, result)
                return result
                
        except (GeocoderTimedOut, GeocoderQuotaExceeded):
            print(f"\nRate limit hit for {address}, waiting longer...")
            time.sleep(5)
            continue
        except Exception as e:
            print(f"\nError geocoding address '{address}': {str(e)}")
            continue

    return None, None

def process_excel_file(input_file, output_file, start_index=None, end_index=None, client_id=1):
    """
    Process Excel file with batch range support.
    """
    try:
        # Initialize cache and geocoders
        cache = GeocodingCache()
        primary_geocoder = ArcGIS(timeout=10)
        backup_geocoder = Nominatim(user_agent=f"my_geocoder_client_{client_id}", timeout=10)
        
        # Read the Excel file
        df = pd.read_excel(input_file)
        
        if 'Daftar Kantor Kelurahan' not in df.columns:
            raise ValueError("Excel file must contain a 'Daftar Kantor Kelurahan' column")

        # Create output directory if it doesn't exist
        os.makedirs('batch_results', exist_ok=True)
        
        # Handle batch processing ranges
        total_records = len(df)
        start_idx = start_index if start_index is not None else 0
        end_idx = min(end_index if end_index is not None else total_records, total_records)
        
        print(f"\nClient {client_id} processing records {start_idx} to {end_idx}")
        
        # Create new columns for coordinates if they don't exist
        if 'Latitude' not in df.columns:
            df['Latitude'] = None
        if 'Longitude' not in df.columns:
            df['Longitude'] = None
        
        # Process only the specified range
        batch_df = df.iloc[start_idx:end_idx].copy()
        addresses = batch_df['Daftar Kantor Kelurahan'].tolist()
        
        for idx, address in enumerate(tqdm(addresses, desc=f"Client {client_id} - Geocoding addresses")):
            global_idx = start_idx + idx
            lat, lng = get_coordinates(address, cache, primary_geocoder, backup_geocoder)
            df.at[global_idx, 'Latitude'] = lat
            df.at[global_idx, 'Longitude'] = lng
            
            # Save intermediate results every 10 records
            if idx % 10 == 0:
                batch_output = f'batch_results/batch_{client_id}_intermediate.xlsx'
                df.to_excel(batch_output, index=False)
        
        # Save final results for this batch
        batch_output = f'batch_results/batch_{client_id}_final.xlsx'
        df.to_excel(batch_output, index=False)
        
        # Also save to main output file if specified
        if output_file:
            df.to_excel(output_file, index=False)
            
        print(f"\nClient {client_id} processing complete. Results saved to {batch_output}")
        
        # Save final cache
        cache.save_cache()
        
    except Exception as e:
        print(f"Error processing Excel file in client {client_id}: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Excel file with geocoding in batches')
    parser.add_argument('--input', default="DATA LAT LONG IDM.xlsx", help='Input Excel file')
    parser.add_argument('--output', default="geocoded.xlsx", help='Output Excel file')
    parser.add_argument('--start', type=int, help='Start index for batch processing')
    parser.add_argument('--end', type=int, help='End index for batch processing')
    parser.add_argument('--client', type=int, default=1, help='Client ID number')
    
    args = parser.parse_args()
    
    process_excel_file(
        args.input,
        args.output,
        start_index=args.start,
        end_index=args.end,
        client_id=args.client
    )