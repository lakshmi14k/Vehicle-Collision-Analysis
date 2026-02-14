"""
Vehicle Collision Data Cleaning Script
Cleans raw data from Austin, Chicago, and NYC
Outputs 3 separate cleaned CSV files
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

print("="*60)
print("VEHICLE COLLISION DATA CLEANING - SCRIPT 1")
print("="*60)

def clean_austin(input_file, output_file):
    """Clean Austin vehicle collision data"""
    print("\n[1/3] Cleaning Austin data...")
    
    df = pd.read_csv(input_file, low_memory=False)
    print(f"   - Loaded {len(df):,} records")
    
    columns_to_keep = [
        'crash_id',
        'crash_date',
        'crash_time',
        'crash_fatal_fl',
        'latitude',
        'longitude',
        'street_name',
        'crash_speed_limit',
        'crash_sev_id',
        'sus_serious_injry_cnt',
        'nonincap_injry_cnt',
        'poss_injry_cnt',
        'non_injry_cnt',
        'tot_injry_cnt',
        'death_cnt',
        'contrib_factr_p1_id',
        'contrib_factr_p2_id',
        'pedestrian_fl',
        'motor_vehicle_fl',
        'motorcycle_fl',
        'bicycle_fl',
        'pedestrian_death_count',
        'pedestrian_serious_injury_count',
        'motor_vehicle_death_count',
        'motor_vehicle_serious_injury_count',
        'bicycle_death_count',
        'bicycle_serious_injury_count',
        'motorcycle_death_count',
        'motorcycle_serious_injury_count'
    ]
    
    df = df[columns_to_keep].copy()
    
    df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')
    df['year'] = df['crash_date'].dt.year
    df['month'] = df['crash_date'].dt.month
    df['day'] = df['crash_date'].dt.day
    df['month_name'] = df['crash_date'].dt.month_name()
    df['crash_date'] = df['crash_date'].dt.date
    
    df['crash_time'] = df['crash_time'].fillna('00:00:00')
    df['latitude'] = df['latitude'].fillna(0)
    df['longitude'] = df['longitude'].fillna(0)
    df['street_name'] = df['street_name'].fillna('UNKNOWN')
    
    flag_columns = ['crash_fatal_fl', 'pedestrian_fl', 'motor_vehicle_fl', 'motorcycle_fl', 'bicycle_fl']
    for col in flag_columns:
        df[col] = df[col].apply(lambda x: True if x == 'Y' else False)
    
    count_columns = [
        'sus_serious_injry_cnt', 'nonincap_injry_cnt', 'poss_injry_cnt',
        'non_injry_cnt', 'tot_injry_cnt', 'death_cnt',
        'pedestrian_death_count', 'pedestrian_serious_injury_count',
        'motor_vehicle_death_count', 'motor_vehicle_serious_injury_count',
        'bicycle_death_count', 'bicycle_serious_injury_count',
        'motorcycle_death_count', 'motorcycle_serious_injury_count'
    ]
    for col in count_columns:
        df[col] = df[col].fillna(0).astype(int)
    
    df['contrib_factr_p1_id'] = df['contrib_factr_p1_id'].fillna('UNKNOWN')
    df['contrib_factr_p2_id'] = df['contrib_factr_p2_id'].fillna('UNKNOWN')
    df['city'] = 'Austin'
    df['di_process_id'] = 'AUSTIN_CLEAN_' + datetime.now().strftime('%Y%m%d')
    df['di_current_date'] = datetime.now()
    
    df.to_csv(output_file, index=False)
    print(f"   ✓ Saved {len(df):,} cleaned records")
    
    return df


def clean_chicago(input_file, output_file):
    """Clean Chicago vehicle collision data"""
    print("\n[2/3] Cleaning Chicago data...")
    
    df = pd.read_csv(input_file, low_memory=False)
    print(f"   - Loaded {len(df):,} records")
    
    columns_to_keep = [
        'CRASH_RECORD_ID',
        'CRASH_DATE',
        'POSTED_SPEED_LIMIT',
        'TRAFFIC_CONTROL_DEVICE',
        'DEVICE_CONDITION',
        'WEATHER_CONDITION',
        'LIGHTING_CONDITION',
        'FIRST_CRASH_TYPE',
        'TRAFFICWAY_TYPE',
        'ALIGNMENT',
        'ROADWAY_SURFACE_COND',
        'ROAD_DEFECT',
        'CRASH_TYPE',
        'DAMAGE',
        'PRIM_CONTRIBUTORY_CAUSE',
        'SEC_CONTRIBUTORY_CAUSE',
        'STREET_NO',
        'STREET_DIRECTION',
        'STREET_NAME',
        'NUM_UNITS',
        'MOST_SEVERE_INJURY',
        'INJURIES_TOTAL',
        'INJURIES_FATAL',
        'INJURIES_INCAPACITATING',
        'INJURIES_NON_INCAPACITATING',
        'INJURIES_REPORTED_NOT_EVIDENT',
        'INJURIES_NO_INDICATION',
        'CRASH_HOUR',
        'CRASH_DAY_OF_WEEK',
        'CRASH_MONTH',
        'LATITUDE',
        'LONGITUDE'
    ]
    
    df = df[columns_to_keep].copy()
    df.columns = df.columns.str.lower()
    
    df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')
    df['crash_time'] = df['crash_date'].dt.strftime('%H:%M:%S')
    df['year'] = df['crash_date'].dt.year
    df['month'] = df['crash_date'].dt.month
    df['day'] = df['crash_date'].dt.day
    df['month_name'] = df['crash_date'].dt.month_name()
    df['crash_date'] = df['crash_date'].dt.date
    
    df['latitude'] = df['latitude'].fillna(0)
    df['longitude'] = df['longitude'].fillna(0)
    df['street_name'] = df['street_name'].fillna('UNKNOWN')
    df['street_direction'] = df['street_direction'].fillna('')
    df['street_no'] = df['street_no'].fillna(0).astype(int)
    
    condition_columns = ['weather_condition', 'lighting_condition', 'roadway_surface_cond', 'device_condition', 'traffic_control_device', 'road_defect']
    for col in condition_columns:
        df[col] = df[col].fillna('UNKNOWN')
    
    injury_columns = ['injuries_total', 'injuries_fatal', 'injuries_incapacitating', 'injuries_non_incapacitating', 'injuries_reported_not_evident', 'injuries_no_indication']
    for col in injury_columns:
        df[col] = df[col].fillna(0).astype(int)
    
    df['prim_contributory_cause'] = df['prim_contributory_cause'].fillna('UNKNOWN')
    df['sec_contributory_cause'] = df['sec_contributory_cause'].fillna('UNKNOWN')
    df['crash_type'] = df['crash_type'].fillna('UNKNOWN')
    df['damage'] = df['damage'].fillna('UNKNOWN')
    df['most_severe_injury'] = df['most_severe_injury'].fillna('NO INDICATION OF INJURY')
    df['city'] = 'Chicago'
    df['di_process_id'] = 'CHICAGO_CLEAN_' + datetime.now().strftime('%Y%m%d')
    df['di_current_date'] = datetime.now()
    
    df.to_csv(output_file, index=False)
    print(f"   ✓ Saved {len(df):,} cleaned records")
    
    return df


def clean_nyc(input_file, output_file):
    """Clean NYC vehicle collision data"""
    print("\n[3/3] Cleaning NYC data...")
    
    df = pd.read_csv(input_file, low_memory=False)
    print(f"   - Loaded {len(df):,} records")
    
    columns_to_keep = [
        'COLLISION_ID',
        'CRASH DATE',
        'CRASH TIME',
        'BOROUGH',
        'ZIP CODE',
        'LATITUDE',
        'LONGITUDE',
        'ON STREET NAME',
        'CROSS STREET NAME',
        'OFF STREET NAME',
        'NUMBER OF PERSONS INJURED',
        'NUMBER OF PERSONS KILLED',
        'NUMBER OF PEDESTRIANS INJURED',
        'NUMBER OF PEDESTRIANS KILLED',
        'NUMBER OF CYCLIST INJURED',
        'NUMBER OF CYCLIST KILLED',
        'NUMBER OF MOTORIST INJURED',
        'NUMBER OF MOTORIST KILLED',
        'CONTRIBUTING FACTOR VEHICLE 1',
        'CONTRIBUTING FACTOR VEHICLE 2',
        'CONTRIBUTING FACTOR VEHICLE 3',
        'VEHICLE TYPE CODE 1',
        'VEHICLE TYPE CODE 2',
        'VEHICLE TYPE CODE 3'
    ]
    
    df = df[columns_to_keep].copy()
    
    df.rename(columns={
        'CRASH DATE': 'crash_date',
        'CRASH TIME': 'crash_time',
        'COLLISION_ID': 'collision_id',
        'BOROUGH': 'borough',
        'ZIP CODE': 'zip_code',
        'LATITUDE': 'latitude',
        'LONGITUDE': 'longitude',
        'ON STREET NAME': 'street_name',
        'CROSS STREET NAME': 'cross_street_name',
        'OFF STREET NAME': 'off_street_name',
        'NUMBER OF PERSONS INJURED': 'persons_injured',
        'NUMBER OF PERSONS KILLED': 'persons_killed',
        'NUMBER OF PEDESTRIANS INJURED': 'pedestrians_injured',
        'NUMBER OF PEDESTRIANS KILLED': 'pedestrians_killed',
        'NUMBER OF CYCLIST INJURED': 'cyclists_injured',
        'NUMBER OF CYCLIST KILLED': 'cyclists_killed',
        'NUMBER OF MOTORIST INJURED': 'motorists_injured',
        'NUMBER OF MOTORIST KILLED': 'motorists_killed',
        'CONTRIBUTING FACTOR VEHICLE 1': 'contrib_factor_1',
        'CONTRIBUTING FACTOR VEHICLE 2': 'contrib_factor_2',
        'CONTRIBUTING FACTOR VEHICLE 3': 'contrib_factor_3',
        'VEHICLE TYPE CODE 1': 'vehicle_type_1',
        'VEHICLE TYPE CODE 2': 'vehicle_type_2',
        'VEHICLE TYPE CODE 3': 'vehicle_type_3'
    }, inplace=True)
    
    df['crash_date'] = pd.to_datetime(df['crash_date'], errors='coerce')
    df['year'] = df['crash_date'].dt.year
    df['month'] = df['crash_date'].dt.month
    df['day'] = df['crash_date'].dt.day
    df['month_name'] = df['crash_date'].dt.month_name()
    df['crash_date'] = df['crash_date'].dt.date
    
    df['crash_time'] = df['crash_time'].fillna('00:00')
    df['latitude'] = df['latitude'].fillna(0)
    df['longitude'] = df['longitude'].fillna(0)
    df['street_name'] = df['street_name'].fillna('UNKNOWN')
    df['cross_street_name'] = df['cross_street_name'].fillna('')
    df['off_street_name'] = df['off_street_name'].fillna('')
    df['borough'] = df['borough'].fillna('UNKNOWN')
    df['zip_code'] = df['zip_code'].fillna('00000')
    
    count_columns = ['persons_injured', 'persons_killed', 'pedestrians_injured', 'pedestrians_killed', 'cyclists_injured', 'cyclists_killed', 'motorists_injured', 'motorists_killed']
    for col in count_columns:
        df[col] = df[col].fillna(0).astype(int)
    
    df['contrib_factor_1'] = df['contrib_factor_1'].fillna('Unspecified')
    df['contrib_factor_2'] = df['contrib_factor_2'].fillna('Unspecified')
    df['contrib_factor_3'] = df['contrib_factor_3'].fillna('Unspecified')
    df['vehicle_type_1'] = df['vehicle_type_1'].fillna('UNKNOWN')
    df['vehicle_type_2'] = df['vehicle_type_2'].fillna('UNKNOWN')
    df['vehicle_type_3'] = df['vehicle_type_3'].fillna('UNKNOWN')
    df['city'] = 'NewYork'
    df['di_process_id'] = 'NYC_CLEAN_' + datetime.now().strftime('%Y%m%d')
    df['di_current_date'] = datetime.now()
    
    df.to_csv(output_file, index=False)
    print(f"   ✓ Saved {len(df):,} cleaned records")
    
    return df


if __name__ == "__main__":
    print("\nStarting data cleaning process...\n")
    
    austin_input = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/Austin_Raw.csv"
    chicago_input = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/Chicago_Raw.csv"
    nyc_input = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/NYC_Raw.csv"
    
    austin_output = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/Austin_Cleaned.csv"
    chicago_output = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/Chicago_Cleaned.csv"
    nyc_output = "C:/Users/laksh/OneDrive/Desktop/Project Cleanup/Vehicle Collision Analysis/Data/NYC_Cleaned.csv"
    
    try:
        austin_df = clean_austin(austin_input, austin_output)
        chicago_df = clean_chicago(chicago_input, chicago_output)
        nyc_df = clean_nyc(nyc_input, nyc_output)
        
        print("\n" + "="*60)
        print("CLEANING COMPLETE!")
        print("="*60)
        print(f"\nSummary:")
        print(f"  Austin:  {len(austin_df):,} records")
        print(f"  Chicago: {len(chicago_df):,} records")
        print(f"  NYC:     {len(nyc_df):,} records")
        print(f"\nTotal:   {len(austin_df) + len(chicago_df) + len(nyc_df):,} records cleaned")
        print("\n✓ Ready for Script 2: Combining data for visualization")
        print("="*60)
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: Could not find input file")
        print(f"   {e}")
        
    except Exception as e:
        print(f"\n❌ ERROR during cleaning: {e}")
        import traceback
        traceback.print_exc()