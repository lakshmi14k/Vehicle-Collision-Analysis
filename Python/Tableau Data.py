"""
Vehicle Collision Data - Script 2
Combines cleaned data from Austin, Chicago, and NYC
Filters for 2022-2024 data only
Keeps only common columns for Tableau visualization
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

print("="*60)
print("VEHICLE COLLISION DATA - SCRIPT 2")
print("Combining Data for Tableau Visualization")
print("="*60)

def load_and_standardize(austin_file, chicago_file, nyc_file):
    """Load all 3 cleaned files and standardize to common columns"""
    
    print("\nLoading cleaned data files...")
    
    # Load Austin
    print("  [1/3] Loading Austin...")
    austin = pd.read_csv(austin_file, low_memory=False)
    print(f"        Loaded {len(austin):,} records")
    
    # Load Chicago
    print("  [2/3] Loading Chicago...")
    chicago = pd.read_csv(chicago_file, low_memory=False)
    print(f"        Loaded {len(chicago):,} records")
    
    # Load NYC
    print("  [3/3] Loading NYC...")
    nyc = pd.read_csv(nyc_file, low_memory=False)
    print(f"        Loaded {len(nyc):,} records")
    
    print(f"\n  Total records loaded: {len(austin) + len(chicago) + len(nyc):,}")
    
    return austin, chicago, nyc


def create_master_dataset(austin, chicago, nyc):
    """Combine all datasets with common columns for visualization"""
    
    print("\nCreating master dataset...")
    
    # Austin - standardize columns
    print("  [1/3] Standardizing Austin columns...")
    austin_viz = pd.DataFrame({
        'accident_id': austin['crash_id'],
        'city': austin['city'],
        'crash_date': pd.to_datetime(austin['crash_date']),
        'crash_time': austin['crash_time'],
        'year': austin['year'],
        'month': austin['month'],
        'month_name': austin['month_name'],
        'latitude': austin['latitude'],
        'longitude': austin['longitude'],
        'street_name': austin['street_name'],
        'total_injuries': austin['tot_injry_cnt'],
        'total_deaths': austin['death_cnt'],
        'pedestrian_injured': austin.get('pedestrian_serious_injury_count', 0),
        'pedestrian_killed': austin.get('pedestrian_death_count', 0),
        'cyclist_injured': austin.get('bicycle_serious_injury_count', 0),
        'cyclist_killed': austin.get('bicycle_death_count', 0),
        'motorist_injured': austin.get('motor_vehicle_serious_injury_count', 0),
        'motorist_killed': austin.get('motor_vehicle_death_count', 0),
        'contributing_factor_1': austin.get('contrib_factr_p1_id', 'UNKNOWN'),
        'contributing_factor_2': austin.get('contrib_factr_p2_id', 'UNKNOWN'),
        'weather_condition': 'UNKNOWN'  # Austin doesn't have weather
    })
    
    # Chicago - standardize columns
    print("  [2/3] Standardizing Chicago columns...")
    chicago_viz = pd.DataFrame({
        'accident_id': chicago['crash_record_id'],
        'city': chicago['city'],
        'crash_date': pd.to_datetime(chicago['crash_date']),
        'crash_time': chicago['crash_time'],
        'year': chicago['year'],
        'month': chicago['month'],
        'month_name': chicago['month_name'],
        'latitude': chicago['latitude'],
        'longitude': chicago['longitude'],
        'street_name': chicago['street_name'],
        'total_injuries': chicago['injuries_total'],
        'total_deaths': chicago['injuries_fatal'],
        'pedestrian_injured': 0,  # Chicago doesn't separate pedestrian injuries
        'pedestrian_killed': 0,
        'cyclist_injured': 0,
        'cyclist_killed': 0,
        'motorist_injured': chicago['injuries_incapacitating'] + chicago['injuries_non_incapacitating'],
        'motorist_killed': chicago['injuries_fatal'],
        'contributing_factor_1': chicago['prim_contributory_cause'],
        'contributing_factor_2': chicago.get('sec_contributory_cause', 'UNKNOWN'),
        'weather_condition': chicago.get('weather_condition', 'UNKNOWN')
    })
    
    # NYC - standardize columns
    print("  [3/3] Standardizing NYC columns...")
    nyc_viz = pd.DataFrame({
        'accident_id': nyc['collision_id'],
        'city': nyc['city'],
        'crash_date': pd.to_datetime(nyc['crash_date']),
        'crash_time': nyc['crash_time'],
        'year': nyc['year'],
        'month': nyc['month'],
        'month_name': nyc['month_name'],
        'latitude': nyc['latitude'],
        'longitude': nyc['longitude'],
        'street_name': nyc['street_name'],
        'total_injuries': nyc['persons_injured'],
        'total_deaths': nyc['persons_killed'],
        'pedestrian_injured': nyc['pedestrians_injured'],
        'pedestrian_killed': nyc['pedestrians_killed'],
        'cyclist_injured': nyc['cyclists_injured'],
        'cyclist_killed': nyc['cyclists_killed'],
        'motorist_injured': nyc['motorists_injured'],
        'motorist_killed': nyc['motorists_killed'],
        'contributing_factor_1': nyc['contrib_factor_1'],
        'contributing_factor_2': nyc.get('contrib_factor_2', 'Unspecified'),
        'weather_condition': 'UNKNOWN'  # NYC doesn't have weather
    })
    
    # Combine all datasets
    print("\n  Combining all datasets...")
    master = pd.concat([austin_viz, chicago_viz, nyc_viz], ignore_index=True)
    print(f"  ✓ Combined dataset: {len(master):,} total records")
    
    return master


def filter_recent_years(master, start_year=2022, end_year=2024):
    """Filter data for recent years only"""
    
    print(f"\nFiltering data for years {start_year}-{end_year}...")
    print(f"  Before filtering: {len(master):,} records")
    
    # Filter by year
    master_filtered = master[
        (master['year'] >= start_year) & 
        (master['year'] <= end_year)
    ].copy()
    
    print(f"  After filtering:  {len(master_filtered):,} records")
    print(f"  Reduction: {len(master) - len(master_filtered):,} records removed")
    print(f"  Percentage kept: {len(master_filtered)/len(master)*100:.1f}%")
    
    return master_filtered


def add_calculated_fields(master):
    """Add useful calculated fields for Tableau"""
    
    print("\nAdding calculated fields...")
    
    # Add season
    master['season'] = master['month'].apply(lambda x: 
        'Winter' if x in [12, 1, 2] else
        'Spring' if x in [3, 4, 5] else
        'Summer' if x in [6, 7, 8] else
        'Fall'
    )
    
    # Add time period
    master['crash_hour'] = pd.to_datetime(master['crash_time'], format='%H:%M:%S', errors='coerce').dt.hour
    master['time_period'] = master['crash_hour'].apply(lambda x:
        'Early Morning' if 0 <= x < 6 else
        'Morning' if 6 <= x < 12 else
        'Afternoon' if 12 <= x < 17 else
        'Evening' if 17 <= x < 21 else
        'Night'
    )
    
    # Add severity flag
    master['has_fatality'] = master['total_deaths'] > 0
    master['has_injury'] = master['total_injuries'] > 0
    
    # Add total casualties
    master['total_casualties'] = master['total_injuries'] + master['total_deaths']
    
    # Clean contributing factors
    master['contributing_factor_1'] = master['contributing_factor_1'].replace('', 'Unspecified')
    master['contributing_factor_1'] = master['contributing_factor_1'].fillna('Unspecified')
    
    print("  ✓ Added: season, time_period, severity flags, total_casualties")
    
    return master


def save_master_file(master, output_file):
    """Save the master visualization file"""
    
    print(f"\nSaving master file...")
    
    # Sort by date
    master = master.sort_values(['crash_date', 'city']).reset_index(drop=True)
    
    # Save to CSV
    master.to_csv(output_file, index=False)
    
    print(f"  ✓ Saved to: {output_file}")
    print(f"  ✓ Total rows: {len(master):,}")
    print(f"  ✓ Total columns: {len(master.columns)}")
    
    # Calculate estimated Tableau Public size
    estimated_cells = len(master) * len(master.columns)
    print(f"\n  Tableau Public estimate:")
    print(f"    Cells: {estimated_cells:,} / 15,000,000 limit")
    print(f"    Usage: {estimated_cells/15000000*100:.1f}%")
    
    if estimated_cells < 15000000:
        print(f"    ✓ Fits in Tableau Public!")
    else:
        print(f"    ⚠ May exceed Tableau Public limit")
    
    return master


def print_summary(master):
    """Print summary statistics"""
    
    print("\n" + "="*60)
    print("MASTER DATASET SUMMARY")
    print("="*60)
    
    print("\nRecords by City:")
    print(master['city'].value_counts().to_string())
    
    print("\nRecords by Year:")
    print(master['year'].value_counts().sort_index().to_string())
    
    print("\nTotal Statistics:")
    print(f"  Total Accidents: {len(master):,}")
    print(f"  Total Injuries:  {master['total_injuries'].sum():,}")
    print(f"  Total Deaths:    {master['total_deaths'].sum():,}")
    print(f"  Accidents with Fatalities: {master['has_fatality'].sum():,}")
    
    print("\nTop Contributing Factors:")
    print(master['contributing_factor_1'].value_counts().head(10).to_string())


if __name__ == "__main__":
    
    print("\nStarting data combination process...\n")
    
    # Input files (cleaned data)
    austin_input = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/Austin_Cleaned.csv"
    chicago_input = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/Chicago_Cleaned.csv"
    nyc_input = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/NYC_Cleaned.csv"
    
    # Output file
    master_output = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/Vehicle_Collisions_Master.csv"
    
    try:
        # Load data
        austin, chicago, nyc = load_and_standardize(austin_input, chicago_input, nyc_input)
        
        # Create master dataset
        master = create_master_dataset(austin, chicago, nyc)
        
        # Filter for recent years (2022-2024)
        master = filter_recent_years(master, start_year=2022, end_year=2024)
        
        # Add calculated fields
        master = add_calculated_fields(master)
        
        # Save master file
        master = save_master_file(master, master_output)
        
        # Print summary
        print_summary(master)
        
        print("\n" + "="*60)
        print("✓ SCRIPT 2 COMPLETE!")
        print("="*60)
        print(f"\nYour master file is ready for Tableau:")
        print(f"  {master_output}")
        print("\nNext step: Load this file into Tableau Public")
        print("="*60)
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: Could not find input file")
        print(f"   {e}")
        print("\n   Make sure Script 1 has been run first!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()