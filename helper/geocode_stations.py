#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

RAW_TRIPS = Path("data/2021-04 (2).csv")  # or point to your raw file
CACHE_PATH = Path("data/geocode_cache.csv")

def geocode_address(addr, geocode_fn):
    if pd.isna(addr):
        return None, None
    try:
        loc = geocode_fn(addr)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception:
        return None, None

def main():
    print("Loading trips to extract unique stations...")
    df = pd.read_csv(RAW_TRIPS, encoding="utf-8-sig")

    stations = pd.concat([
        df[['Departure station id', 'Departure station name']].rename(
            columns={'Departure station id': 'station_id',
                     'Departure station name': 'station_name'}),
        df[['Return station id', 'Return station name']].rename(
            columns={'Return station id': 'station_id',
                     'Return station name': 'station_name'}),
    ]).drop_duplicates(subset=['station_id']).reset_index(drop=True)

    if CACHE_PATH.exists():
        print(f"Cache exists at {CACHE_PATH}; exiting.")
        return

    print(f"Geocoding {len(stations)} stations (Helsinki, Finland)...")
    geolocator = Nominatim(user_agent="helsinki-bike-geocoder")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    coords = []
    for name in tqdm(stations['station_name'], desc="Geocoding"):
        coords.append(geocode_address(f"{name}, Helsinki, Finland", geocode))

    stations[['lat', 'lon']] = pd.DataFrame(coords, index=stations.index)
    stations.to_csv(CACHE_PATH, index=False)
    print(f"Saved cache to {CACHE_PATH}")

if __name__ == "__main__":
    main()
