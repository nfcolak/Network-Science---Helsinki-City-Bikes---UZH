#!/usr/bin/env python3
"""
Data Quality Analysis for Helsinki City Bikes Dataset
Analyzes the 2021-04 (2).csv file and identifies data quality issues
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Load the data
print("Loading data...")
df = pd.read_csv('data/2021-04_cleaned.csv', encoding='utf-8-sig')

print(f"\n{'='*80}")
print("DATASET OVERVIEW")
print(f"{'='*80}")
print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print(f"\nColumn names:")
for i, col in enumerate(df.columns, 1):
    print(f"  {i}. {col}")

print(f"\n{'='*80}")
print("DATA TYPES")
print(f"{'='*80}")
print(df.dtypes)

print(f"\n{'='*80}")
print("MISSING VALUES")
print(f"{'='*80}")
missing = df.isnull().sum()
missing_pct = (missing / len(df)) * 100
missing_df = pd.DataFrame({
    'Missing Count': missing,
    'Percentage': missing_pct
})
print(missing_df[missing_df['Missing Count'] > 0])

print(f"\n{'='*80}")
print("DUPLICATE ROWS")
print(f"{'='*80}")
duplicates = df.duplicated().sum()
print(f"Number of duplicate rows: {duplicates:,}")

print(f"\n{'='*80}")
print("BASIC STATISTICS")
print(f"{'='*80}")
print(df.describe())

# Convert datetime columns
print(f"\n{'='*80}")
print("TEMPORAL ANALYSIS")
print(f"{'='*80}")
df['Departure'] = pd.to_datetime(df['Departure'], format='mixed', errors='coerce')
df['Return'] = pd.to_datetime(df['Return'], format='mixed', errors='coerce')

print(f"Date range: {df['Departure'].min()} to {df['Departure'].max()}")
print(f"Return range: {df['Return'].min()} to {df['Return'].max()}")

# Calculate actual duration
df['Calculated_Duration'] = (df['Return'] - df['Departure']).dt.total_seconds()
df['Duration_Diff'] = df['Calculated_Duration'] - df['Duration (sec.)']

print(f"\n{'='*80}")
print("DATA QUALITY ISSUES")
print(f"{'='*80}")

# Issue 1: Negative durations
negative_duration = df[df['Duration (sec.)'] < 0]
print(f"\n1. Negative durations: {len(negative_duration):,} rows")

# Issue 2: Return before departure
return_before_departure = df[df['Return'] < df['Departure']]
print(f"2. Return before departure: {len(return_before_departure):,} rows")

# Issue 3: Very short trips (< 10 seconds)
very_short = df[df['Duration (sec.)'] < 10]
print(f"3. Very short trips (<10 sec): {len(very_short):,} rows ({len(very_short)/len(df)*100:.2f}%)")

# Issue 4: Very long trips (> 24 hours)
very_long = df[df['Duration (sec.)'] > 86400]
print(f"4. Very long trips (>24 hours): {len(very_long):,} rows ({len(very_long)/len(df)*100:.2f}%)")

# Issue 5: Zero distance
zero_distance = df[df['Covered distance (m)'] == 0]
print(f"5. Zero distance trips: {len(zero_distance):,} rows ({len(zero_distance)/len(df)*100:.2f}%)")

# Issue 6: Same departure and return station with significant distance
same_station = df[df['Departure station id'] == df['Return station id']]
same_station_with_distance = same_station[same_station['Covered distance (m)'] > 100]
print(f"6. Same station but distance > 100m: {len(same_station_with_distance):,} rows")

# Issue 7: Duration mismatch
duration_mismatch = df[abs(df['Duration_Diff']) > 1]  # More than 1 second difference
print(f"7. Duration mismatch (calc vs recorded): {len(duration_mismatch):,} rows ({len(duration_mismatch)/len(df)*100:.2f}%)")

# Issue 8: Unrealistic speed (> 50 km/h for bikes)
df['Speed_kmh'] = (df['Covered distance (m)'] / 1000) / (df['Duration (sec.)'] / 3600)
unrealistic_speed = df[df['Speed_kmh'] > 50]
print(f"8. Unrealistic speed (>50 km/h): {len(unrealistic_speed):,} rows ({len(unrealistic_speed)/len(df)*100:.2f}%)")

# Issue 9: Very slow speed (< 1 km/h for trips > 5 min)
long_slow = df[(df['Duration (sec.)'] > 300) & (df['Speed_kmh'] < 1)]
print(f"9. Very slow speed (<1 km/h, >5 min): {len(long_slow):,} rows ({len(long_slow)/len(df)*100:.2f}%)")

print(f"\n{'='*80}")
print("STATION ANALYSIS")
print(f"{'='*80}")
print(f"Unique departure stations: {df['Departure station id'].nunique()}")
print(f"Unique return stations: {df['Return station id'].nunique()}")
print(f"Unique station names (departure): {df['Departure station name'].nunique()}")
print(f"Unique station names (return): {df['Return station name'].nunique()}")

print(f"\n{'='*80}")
print("TOP 10 MOST POPULAR DEPARTURE STATIONS")
print(f"{'='*80}")
print(df['Departure station name'].value_counts().head(10))

print(f"\n{'='*80}")
print("TOP 10 MOST POPULAR RETURN STATIONS")
print(f"{'='*80}")
print(df['Return station name'].value_counts().head(10))

print(f"\n{'='*80}")
print("SUMMARY OF RECOMMENDED CLEANING STEPS")
print(f"{'='*80}")
print("""
1. Remove or flag rows with negative durations
2. Remove or flag rows where return time < departure time
3. Consider removing very short trips (<10 seconds) - likely false starts
4. Review very long trips (>24 hours) - bikes may not have been properly returned
5. Handle zero-distance trips appropriately (round trips at same station)
6. Fix duration mismatches between calculated and recorded values
7. Remove outliers with unrealistic speeds (>50 km/h)
8. Review suspiciously slow trips
9. Check for and remove any duplicate entries
10. Standardize station names if inconsistencies exist
""")

# Save sample of problematic rows for inspection
print(f"\n{'='*80}")
print("SAVING SAMPLE PROBLEMATIC ROWS")
print(f"{'='*80}")

if len(very_short) > 0:
    print("Saving sample of very short trips...")
    very_short.head(20).to_csv('data/sample_short_trips.csv', index=False)

if len(very_long) > 0:
    print("Saving sample of very long trips...")
    very_long.head(20).to_csv('data/sample_long_trips.csv', index=False)

if len(unrealistic_speed) > 0:
    print("Saving sample of unrealistic speed trips...")
    unrealistic_speed.head(20).to_csv('data/sample_fast_trips.csv', index=False)

print("\nAnalysis complete!")
