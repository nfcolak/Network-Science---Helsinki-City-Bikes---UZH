#!/usr/bin/env python3
"""Build a station coordinate cache from raw Helsinki City Bikes trips."""

from argparse import ArgumentParser
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_CANDIDATES = [
    PROJECT_ROOT / "data" / "raw" / "2021-04.csv",
    PROJECT_ROOT / "data" / "raw" / "2021-04 (2).csv",
]
DEFAULT_CACHE = PROJECT_ROOT / "data" / "reference" / "geocode_cache.csv"


def default_raw_path():
    for candidate in RAW_CANDIDATES:
        if candidate.exists():
            return candidate
    return RAW_CANDIDATES[0]


def geocode_address(addr, geocode_fn):
    if pd.isna(addr):
        return None, None
    try:
        loc = geocode_fn(addr)
        return (loc.latitude, loc.longitude) if loc else (None, None)
    except Exception:
        return None, None


def parse_args():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=default_raw_path())
    parser.add_argument("--output", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.input.exists():
        expected = " or ".join(str(path) for path in RAW_CANDIDATES)
        raise FileNotFoundError(
            f"Raw trip data not found. Put it at {expected}, or pass --input."
        )

    print("Loading trips to extract unique stations...")
    df = pd.read_csv(args.input, encoding="utf-8-sig")

    stations = pd.concat([
        df[['Departure station id', 'Departure station name']].rename(
            columns={'Departure station id': 'station_id',
                     'Departure station name': 'station_name'}),
        df[['Return station id', 'Return station name']].rename(
            columns={'Return station id': 'station_id',
                     'Return station name': 'station_name'}),
    ]).drop_duplicates(subset=['station_id']).reset_index(drop=True)

    if args.output.exists() and not args.overwrite:
        print(f"Cache exists at {args.output}; pass --overwrite to rebuild it.")
        return

    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter
    from tqdm import tqdm

    print(f"Geocoding {len(stations)} stations (Helsinki, Finland)...")
    geolocator = Nominatim(user_agent="helsinki-bike-geocoder")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    coords = []
    for name in tqdm(stations['station_name'], desc="Geocoding"):
        coords.append(geocode_address(f"{name}, Helsinki, Finland", geocode))

    stations[['lat', 'lon']] = pd.DataFrame(coords, index=stations.index)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    stations.to_csv(args.output, index=False)
    print(f"Saved cache to {args.output}")

if __name__ == "__main__":
    main()
