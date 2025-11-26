#!/usr/bin/env python3
"""
Data Cleaning Script for Helsinki City Bikes Dataset
Cleans the 2021-04 (2).csv file based on identified data quality issues
"""

import pandas as pd
import numpy as np
from datetime import datetime

# Load the data
print("Loading data...")
df = pd.read_csv('data/2021-04 (2).csv', encoding='utf-8-sig')
initial_rows = len(df)
print(f"Initial dataset: {initial_rows:,} rows")

# Convert datetime columns
df['Departure'] = pd.to_datetime(df['Departure'], format='mixed', errors='coerce')
df['Return'] = pd.to_datetime(df['Return'], format='mixed', errors='coerce')

print("\n" + "="*80)
print("CLEANING STEPS")
print("="*80)

# Step 1: Remove rows with invalid datetime
print("\n1. Removing rows with invalid datetime values...")
before = len(df)
df = df.dropna(subset=['Departure', 'Return'])
removed = before - len(df)
print(f"   Removed: {removed:,} rows")
print(f"   Remaining: {len(df):,} rows")

# Step 2: Remove duplicate rows
print("\n2. Removing duplicate rows...")
before = len(df)
df = df.drop_duplicates()
removed = before - len(df)
print(f"   Removed: {removed:,} rows")
print(f"   Remaining: {len(df):,} rows")

# Step 3: Remove rows where return time is before departure time
print("\n3. Removing rows where return < departure...")
before = len(df)
df = df[df['Return'] >= df['Departure']]
removed = before - len(df)
print(f"   Removed: {removed:,} rows")
print(f"   Remaining: {len(df):,} rows")

# Step 4: Calculate a new duration column from timestamps
print("\n4. Calculating new duration from timestamps...")
df['new_duration'] = (df['Return'] - df['Departure']).dt.total_seconds().astype(int)
print(f"   new_duration column added based on timestamps")
print(f"   Remaining: {len(df):,} rows")

# Step 5: Remove very short trips (< 1 minute = 60 seconds)
# These are likely false starts or data entry errors
print("\n5. Removing very short trips (<1 minute)...")
before = len(df)
df = df[df['new_duration'] >= 60]
removed = before - len(df)
print(f"   Removed: {removed:,} rows ({removed/before*100:.2f}%)")
print(f"   Remaining: {len(df):,} rows")

# Step 6: Remove very long trips (> 4 hours = 14400 seconds)
# These likely indicate bikes that weren't properly returned
print("\n6. Removing very long trips (>4 hours)...")
before = len(df)
df = df[df['new_duration'] <= 14400]
removed = before - len(df)
print(f"   Removed: {removed:,} rows ({removed/before*100:.2f}%)")
print(f"   Remaining: {len(df):,} rows")

# Step 7: Handle missing distance values
print("\n7. Handling missing distance values...")
before = df['Covered distance (m)'].isnull().sum()
# Fill with 0 for same-station trips
same_station_mask = df['Departure station id'] == df['Return station id']
df.loc[same_station_mask & df['Covered distance (m)'].isnull(), 'Covered distance (m)'] = 0
# Drop remaining rows with missing distance
df = df.dropna(subset=['Covered distance (m)'])
after = before - df['Covered distance (m)'].isnull().sum()
print(f"   Fixed/removed: {after:,} missing values")
print(f"   Remaining: {len(df):,} rows")

# Step 7.5: Remove trips with zero distance
# These are likely GPS errors or cancelled trips
print("\n7.5. Removing trips with zero distance...")
before = len(df)
df = df[df['Covered distance (m)'] > 0]
removed = before - len(df)
print(f"   Removed: {removed:,} rows ({removed/before*100:.2f}%)")
print(f"   Remaining: {len(df):,} rows")

# Step 8: Remove outliers based on speed
# Calculate speed in km/h
df['Speed_kmh'] = (df['Covered distance (m)'] / 1000) / (df['new_duration'] / 3600)

# Remove trips with unrealistic speeds (> 50 km/h for bikes)
print("\n8. Removing trips with unrealistic speed (>50 km/h)...")
before = len(df)
df = df[df['Speed_kmh'] <= 50]
removed = before - len(df)
print(f"   Removed: {removed:,} rows")
print(f"   Remaining: {len(df):,} rows")

# Drop the temporary speed column
df = df.drop(columns=['Speed_kmh'])

# Step 9: Remove trips with zero duration after cleaning
print("\n9. Removing trips with zero duration...")
before = len(df)
df = df[df['new_duration'] > 0]
removed = before - len(df)
print(f"   Removed: {removed:,} rows")
print(f"   Remaining: {len(df):,} rows")

# Step 10: Sort by departure time
print("\n10. Sorting by departure time...")
df = df.sort_values('Departure', ascending=False).reset_index(drop=True)
print(f"   Sorted {len(df):,} rows")

# Format datetime columns back to string for CSV export
df['Departure'] = df['Departure'].dt.strftime('%Y-%m-%dT%H:%M:%S')
df['Return'] = df['Return'].dt.strftime('%Y-%m-%dT%H:%M:%S')

# Save cleaned data
output_file = 'data/2021-04_cleaned.csv'
print(f"\n{'='*80}")
print("SAVING CLEANED DATA")
print(f"{'='*80}")
df.to_csv(output_file, index=False, encoding='utf-8')
print(f"Cleaned data saved to: {output_file}")

# Summary
print(f"\n{'='*80}")
print("CLEANING SUMMARY")
print(f"{'='*80}")
print(f"Initial rows:        {initial_rows:>10,}")
print(f"Final rows:          {len(df):>10,}")
print(f"Rows removed:        {initial_rows - len(df):>10,}")
print(f"Percentage retained: {(len(df)/initial_rows)*100:>10.2f}%")

print(f"\n{'='*80}")
print("CLEANED DATA STATISTICS")
print(f"{'='*80}")
print(f"\nDuration statistics (seconds):")
print(f"  Mean:   {df['Duration (sec.)'].mean():>10.1f}")
print(f"  Median: {df['Duration (sec.)'].median():>10.1f}")
print(f"  Min:    {df['Duration (sec.)'].min():>10.1f}")
print(f"  Max:    {df['Duration (sec.)'].max():>10.1f}")

print(f"\nDistance statistics (meters):")
print(f"  Mean:   {df['Covered distance (m)'].mean():>10.1f}")
print(f"  Median: {df['Covered distance (m)'].median():>10.1f}")
print(f"  Min:    {df['Covered distance (m)'].min():>10.1f}")
print(f"  Max:    {df['Covered distance (m)'].max():>10.1f}")

print(f"\nUnique stations:")
print(f"  Departure stations: {df['Departure station id'].nunique()}")
print(f"  Return stations:    {df['Return station id'].nunique()}")

print("\nData cleaning complete!")
