#!/usr/bin/env python3
"""Merge cleaned trip data with cached station coordinates."""

from argparse import ArgumentParser
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLEANED = PROJECT_ROOT / "data" / "processed" / "2021-04_cleaned.csv"
DEFAULT_GEOCODE = PROJECT_ROOT / "data" / "reference" / "geocode_cache.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "processed" / "2021-04_merged.csv"
COORDINATE_COLUMNS = [
    "Departure_lat",
    "Departure_lon",
    "Return_lat",
    "Return_lon",
    "departure_lat",
    "departure_lon",
    "return_lat",
    "return_lon",
]


def parse_args():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--trips", type=Path, default=DEFAULT_CLEANED)
    parser.add_argument("--geocode", type=Path, default=DEFAULT_GEOCODE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main():
    args = parse_args()

    geocode = pd.read_csv(args.geocode)
    print(f"Geocode cache: {len(geocode):,} stations")

    trips = pd.read_csv(args.trips)
    print(f"Trips data: {len(trips):,} trips")

    trips = trips.drop(columns=COORDINATE_COLUMNS, errors="ignore")

    trips = trips.merge(
        geocode[["station_id", "lat", "lon"]],
        left_on="Departure station id",
        right_on="station_id",
        how="left",
    ).rename(columns={"lat": "departure_lat", "lon": "departure_lon"}).drop(
        "station_id", axis=1
    )

    trips = trips.merge(
        geocode[["station_id", "lat", "lon"]],
        left_on="Return station id",
        right_on="station_id",
        how="left",
    ).rename(columns={"lat": "return_lat", "lon": "return_lon"}).drop(
        "station_id", axis=1
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    trips.to_csv(args.output, index=False)
    print(f"\nMerged data saved to: {args.output}")
    print(f"Columns: {list(trips.columns)}")

    missing_departure = trips["departure_lat"].isna().sum()
    missing_return = trips["return_lat"].isna().sum()
    print(f"\nMissing departure coordinates: {missing_departure:,}")
    print(f"Missing return coordinates: {missing_return:,}")


if __name__ == "__main__":
    main()
