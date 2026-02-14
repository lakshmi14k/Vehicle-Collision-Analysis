**Vehicle Collision Analysis**

**Problem Statement:** Urban transportation departments struggle to reduce traffic fatalities and improve road safety due to:
- High volume of collision incidents requiring investigation
- Inconsistent accident reporting across jurisdictions
- Limited visibility into contributing factors and patterns
- Reactive rather than proactive safety interventions
- Fragmented data collection standards

**Business Impact:** Inefficient allocation of traffic safety resources leads to preventable accidents, increased fatalities, and millions in economic losses from property damage and medical costs.

**Proposed Solution:** A dimensional data warehouse integrating collision data from three major US cities, enabling:
- Standardized accident tracking and analysis
- Pattern identification for preventive interventions
- Comparative analysis across jurisdictions
- Data-driven traffic safety policy decisions
- Real-time monitoring of high-risk locations

**Key Findings:**

**Austin (147,750 collisions analyzed - 2014-2024):**
- Fatal crashes represent 0.6% of total incidents
- Contributing factors missing in 80.6% of records (data quality issue)
- Pedestrian-involved crashes: 3,505 incidents
- Average crash severity: moderate injuries

**Chicago (817,723 collisions analyzed - 2013-2024):**
- 68.7% of records missing hit-and-run data
- Top contributing cause: Driver skills/knowledge/experience
- Work zone crashes: 1,052 incidents (requires targeted safety measures)
- Injury-causing crashes: 599,383 (73% of total)

**NYC (2,075,427 collisions analyzed - 2012-2024):**
- Largest dataset with most comprehensive tracking
- Missing data primarily in secondary vehicle fields (expected pattern)
- Borough-level analysis shows concentration in Manhattan and Brooklyn
- Contributing factors well-documented for primary vehicles

**Cross-City Insights:**
- Data quality varies significantly (Chicago most complete, Austin least)
- NYC has highest volume but lower fatality rate
- Contributing factor taxonomies need standardization across jurisdictions
- Temporal patterns show peak accident times during rush hours

**Dataset Overview:**

**Austin Traffic Crashes:**
- **Source:** Austin Transportation Department
- **Records:** 147,750 collisions
- **Time Period:** March 2014 - March 2024
- **Variables:** 54 columns (crash details, location, injuries, contributing factors)
- **Missing Data:** 21.6% overall

**Chicago Traffic Crashes:**
- **Source:** Chicago Police Department
- **Records:** 817,723 collisions
- **Time Period:** March 2013 - March 2024
- **Variables:** 48 columns (crash type, weather, road conditions, injuries)
- **Missing Data:** 21.1% overall

**NYC Motor Vehicle Collisions:**
- **Source:** NYC Police Department
- **Records:** 2,075,427 collisions
- **Time Period:** July 2012 - March 2024
- **Variables:** 28 columns (location, casualties, contributing factors, vehicle types)
- **Missing Data:** 29.5% overall

**Data Quality Challenges**
- **Missing values** (21-30% across datasets)
- **Inconsistent contributing factor codes** (each city uses different taxonomy)
- **Coordinate accuracy** (11-13% missing lat/long in some datasets)
- **Multi-vehicle tracking** (NYC tracks up to 5 vehicles, others less granular)
- **Weather data** (only Chicago tracks weather conditions)

**Tech Stack:**
- **ETL:** Python (pandas), Talend Open Studio
- **Database:** Microsoft SQL Server
- **Data Modeling:** Star schema dimensional design
- **Visualization:** Tableau Public, Power BI
- **Data Profiling:** ydata-profiling, Alteryx Designer

**Project Structure**

```
Vehicle-Collision-Analysis/
├── README.md
├── Data/
│   ├── Raw/
│   │   ├── Austin.csv                     Austin collisions (148K records)
│   │   ├── Chicago.csv                    Chicago crashes (818K records)
│   │   └── NYC.csv                        NYC collisions (2.08M records)
│   ├── Cleaned/
│   │   ├── Austin_Cleaned.csv             Standardized Austin data
│   │   ├── Chicago_Cleaned.csv            Standardized Chicago data
│   │   └── NYC_Cleaned.csv                Standardized NYC data
│   └── Master/
│       └── Vehicle_Collisions_Master.csv  Combined 2022-2024 data (491K records)
├── Python/
│   ├── 01_clean_raw_data.py              Data cleaning script
│   └── 02_combine_for_tableau.py         Master file creation
├── ETL/
│   ├── Austin/                            Austin Talend workflows
│   ├── Chicago/                           Chicago Talend workflows
│   ├── NYC/                               NYC Talend workflows
│   └── ETL_Documentation.pdf              Complete ETL process documentation
├── Profiling/
│   ├── Austin_Profiling.yxmd              Alteryx profiling workflow
│   ├── Chicago_Profiling.yxmd             Alteryx profiling workflow
│   └── NYC_Profiling.yxmd                 Alteryx profiling workflow
├── Model/
│   ├── Dimensional_Model.png              Star schema diagram
│   └── DDL_Scripts.sql                    Database creation scripts
└── Dashboards/
    ├── Overview_and_Hotspots.twbx         Tableau Dashboard 1
    └── Deep_Dive_Analysis.twbx            Tableau Dashboard 2
```

**Database Schema: Star Schema Design**

**Fact Tables (2):**
- **Fct_Accident** - Core collision events with casualties and outcomes
- **BridgeContributingFactor** - Many-to-many relationship for contributing factors

**Dimension Tables (6):**
- **Dim_Location** - Street addresses, coordinates, boroughs/beats
- **Dim_Date** - Date hierarchy (year, quarter, month, season, weekday)
- **Dim_Time** - Time of day hierarchy (hour, period, rush hour flag)
- **Dim_VehicleType** - Vehicle classifications (passenger, truck, motorcycle, bicycle)
- **Dim_ContributingFactors** - Accident causes (driver error, weather, road conditions)
- **Dim_WeatherConditions** - Weather at time of crash (Chicago only)

**Key Relationships:**
```
Dim_Location ──┐
Dim_Date ──────┤
Dim_Time ──────┼──> Fct_Accident ──> BridgeContributingFactor ──> Dim_ContributingFactors
Dim_VehicleType┤                              
Dim_Weather ───┘                              
```

**Total Records in Warehouse:**
- 3,040,900 collision records
- 491,174 records (2022-2024 filtered for Tableau)
- 4.2M+ contributing factor relationships
- 10,000+ unique street locations

**Technical Implementation**
 
 ETL Pipeline
**Phase 1: Data Extraction**
- Downloaded CSV/TSV files from city open data portals
- Initial profiling with ydata-profiling and Alteryx
- Identified 3M+ total records spanning 10+ years

**Phase 2: Data Quality Assessment**

Identified issues:
- Missing contributing factors (80% Austin, 15% Chicago, 92% NYC for secondary factors)
- Inconsistent date/time formats across datasets
- Coordinate accuracy issues (11-13% missing)
- Free-text violation descriptions requiring normalization
- Multiple vehicle tracking complexity

**Phase 3: Data Transformation**

**Austin Processing:**
- Separated crash_date into date and time components
- Normalized flag columns (Y/N → Boolean)
- Handled 93% missing latitude/longitude
- Standardized injury count fields
- Mapped contributing factor codes

**Chicago Processing:**
- Extracted time from datetime field
- Converted uppercase column names to lowercase
- Handled 75% missing lane count data
- Standardized weather and lighting conditions
- Consolidated injury severity categories

**NYC Processing:**
- Renamed columns for consistency (spaces → underscores)
- Normalized multi-vehicle data (5 vehicle slots)
- Handled 99% missing data in vehicles 4-5 (expected)
- Consolidated contributing factors across vehicles
- Borough-level aggregation for geographic analysis

**Phase 4: Data Integration**
- Created common schema across 3 cities
- Filtered for 2022-2024 data (Tableau Public optimization)
- Added calculated fields (season, time_period, severity flags)
- Generated master visualization dataset (491K records)

**Phase 5: Data Loading**
- Loaded staged data into SQL Server
- Populated dimensional model using Talend
- Implemented SCD Type 2 for Dim_Location (tracking street name changes)
- Validated referential integrity and data quality

**Data Profiling Results**

**Austin:**
- 54 variables, 147,750 observations
- 1.7M missing cells (21.6%)
- 0 duplicates
- Key issues: Contributing factors missing in 80%+ of records

**Chicago:**
- 48 variables, 817,723 observations
- 8.3M missing cells (21.1%)
- 0 duplicates
- Key issues: Work zone data 99% missing, lane count 75% missing

**NYC:**
- 29 variables, 2,075,427 observations
- 17.8M missing cells (29.5%)
- 0 duplicates
- Key issues: Secondary/tertiary vehicle data sparse (expected pattern)

**SQL Capabilities Demonstrated**

- **Dimensional Modeling:** Star schema with 6 dimensions, 2 facts, 1 bridge table
- **DDL Design:** Foreign keys, constraints, covering indexes
- **Data Validation:** Row count reconciliation, referential integrity checks
- **ETL Logic:** Multi-source staging → transformation → dimensional model
- **Data Quality:** Profiling, null handling, duplicate detection
- **Performance Optimization:** Indexed lookups, efficient joins
- **Bridge Tables:** Many-to-many relationship handling (accidents ↔ contributing factors)

**Visualizations**

**Tableau Dashboards:**

**Dashboard 1: Overview & Hotspots**
- KPIs: Total accidents, fatalities, injuries, fatality rate
- Interactive city-level map showing accident concentrations
- Time trend analysis (monthly patterns 2022-2024)
- Top contributing factors analysis

**Dashboard 2: Deep Dive Analysis**
- Weather impact on accident frequency
- Severity analysis (injuries vs fatalities by city)
- Time period breakdown (morning, afternoon, evening, night)
- Deadliest streets across all three cities

**For interactive dashboards, click [here](https://public.tableau.com/app/profile/lakshmi14k/viz/VehicleCollisionAnalysis/Overview)**

**Installation & Reproducibility:**

**Prerequisites:**
- Python 3.8+ (pandas, numpy, datetime)
- Microsoft SQL Server (optional - for dimensional model)
- Tableau Public or Desktop

** Setup Instructions**

**1. Clone repository:**
```bash
git clone https://github.com/lakshmi14k/Vehicle-Collision-Analysis.git
cd Vehicle-Collision-Analysis
```

**2. Run Python cleaning scripts:**
```bash
 Clean raw data for each city
python Python/01_clean_raw_data.py

Combine and prepare for visualization
python Python/02_combine_for_tableau.py
```

**Outputs:**
- `Data/Cleaned/Austin_Cleaned.csv`
- `Data/Cleaned/Chicago_Cleaned.csv`
- `Data/Cleaned/NYC_Cleaned.csv`
- `Data/Master/Vehicle_Collisions_Master.csv` (ready for Tableau)


**3. Load data using ETL tool:**
- **Talend:** Use workflows in `ETL/` folder
- **Alternative:** Load CSVs directly using Python pandas or SSIS

**4. Open Tableau dashboards:**
- Connect to `Vehicle_Collisions_Master.csv` in Tableau Public
- Or connect to SQL Server database for full dimensional model

**Results:**

**Data Pipeline:**
- 3,040,900 total collision records processed
- 491,174 records (2022-2024) prepared for visualization
- 3 city datasets standardized and integrated
- 10 dimension/fact tables populated

**Insights Delivered:**
- Identified peak accident times and seasonal patterns
- Documented top contributing factors by city
- Mapped high-risk streets requiring safety interventions
- Enabled year-over-year trend analysis
- Quantified pedestrian/cyclist/motorist casualty rates

**Business Value:**
- Enabled data-driven traffic safety policy decisions
- Identified high-risk locations for targeted interventions
- Provided comparative benchmarking across major cities
- Reduced data silos through standardized integration
- Created foundation for predictive accident modeling

**Key Technical Achievements:**
- **Multi-Source Integration:** Combined 3 disparate datasets (3M+ records, different schemas)
- **Data Quality Management:** Handled 20-30% missing data without losing analytical value
- **Schema Standardization:** Unified 3 different column structures into common format
- **Dimensional Modeling:** Star schema with proper grain, SCD Type 2, and bridge tables
- **ETL Optimization:** Python + Talend hybrid approach for scalability
- **Visualization Strategy:** Filtered 3M → 491K records to fit Tableau Public constraints
- **Performance:** Processed 3M+ records with complex transformations in under 5 minutes

**Built with** SQL Server, Python, Alteryx, Talend, Tableau, and dimensional data modeling best practices
