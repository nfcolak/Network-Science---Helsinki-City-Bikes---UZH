import pandas as pd
import os
from pathlib import Path

def divide_temporal_data(input_file, output_dir):
    """
    Divide the bike sharing data into temporal segments.

    Parameters:
    - input_file: Path to the cleaned CSV file
    - output_dir: Directory to save the temporal CSV files
    """
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Read the data
    print(f"Reading data from {input_file}...")
    df = pd.read_csv(input_file)

    # Convert Departure column to datetime
    df['Departure'] = pd.to_datetime(df['Departure'])

    # Extract time components
    df['hour'] = df['Departure'].dt.hour
    df['day_of_week'] = df['Departure'].dt.dayofweek  # Monday=0, Sunday=6
    df['day_name'] = df['Departure'].dt.day_name()

    print(f"Total records: {len(df)}")

    # 1. Night data (20:00 - 06:00)
    print("\nCreating clean_night.csv...")
    night_data = df[(df['hour'] >= 20) | (df['hour'] < 6)]
    night_output = os.path.join(output_dir, 'clean_night.csv')
    night_data.drop(['hour', 'day_of_week', 'day_name'], axis=1).to_csv(night_output, index=False)
    print(f"  - Saved {len(night_data)} records")

    # 2. Day data (06:00 - 20:00)
    print("Creating clean_day.csv...")
    day_data = df[(df['hour'] >= 6) & (df['hour'] < 20)]
    day_output = os.path.join(output_dir, 'clean_day.csv')
    day_data.drop(['hour', 'day_of_week', 'day_name'], axis=1).to_csv(day_output, index=False)
    print(f"  - Saved {len(day_data)} records")

    # 3. Weekday data (Monday=0 to Friday=4)
    print("Creating clean_weekday.csv...")
    weekday_data = df[df['day_of_week'] < 5]
    weekday_output = os.path.join(output_dir, 'clean_weekday.csv')
    weekday_data.drop(['hour', 'day_of_week', 'day_name'], axis=1).to_csv(weekday_output, index=False)
    print(f"  - Saved {len(weekday_data)} records")

    # 4. Weekend data (Saturday=5, Sunday=6)
    print("Creating clean_weekend.csv...")
    weekend_data = df[df['day_of_week'] >= 5]
    weekend_output = os.path.join(output_dir, 'clean_weekend.csv')
    weekend_data.drop(['hour', 'day_of_week', 'day_name'], axis=1).to_csv(weekend_output, index=False)
    print(f"  - Saved {len(weekend_data)} records")

    # 5. Individual day files
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    print("\nCreating individual day files...")
    for day in days:
        day_file = f"clean_{day.lower()}.csv"
        print(f"Creating {day_file}...")
        day_df = df[df['day_name'] == day]
        day_output = os.path.join(output_dir, day_file)
        day_df.drop(['hour', 'day_of_week', 'day_name'], axis=1).to_csv(day_output, index=False)
        print(f"  - Saved {len(day_df)} records")

    print(f"\n✓ All temporal data files created successfully in {output_dir}")
    print(f"\nCreated files:")
    print("  - clean_night.csv")
    print("  - clean_day.csv")
    print("  - clean_weekday.csv")
    print("  - clean_weekend.csv")
    for day in days:
        print(f"  - clean_{day.lower()}.csv")

if __name__ == "__main__":
    # Define paths
    input_file = "data/2021-04_cleaned.csv"
    output_dir = "data/temporal"

    # Run the division
    divide_temporal_data(input_file, output_dir)
