# Actor Database System - Technical Documentation

## Overview
This project implements a comprehensive actor database system that tracks actor performances and their quality classifications over time. The system uses Slowly Changing Dimension (SCD) Type 2 methodology to maintain historical data accuracy and temporal consistency.

## Core Concepts

### 1. Custom Data Types
The system utilizes PostgreSQL custom types to ensure data integrity and enhance code maintainability:

- `film_details`: A composite type that encapsulates film information
  ```sql
  film: text
  votes: integer
  rating: real
  filmid: text
  ```

- `quality_class`: An enumerated type for actor classification
  ```sql
  ENUM ('star', 'good', 'average', 'bad')
  ```

- `actor_scd_type`: A composite type for SCD tracking
  ```sql
  quality_class: quality_class
  is_active: boolean
  start_date: integer
  end_date: integer
  ```

### 2. Main Tables

#### actors Table
Primary table storing current actor information:
- Maintains current state of each actor
- Uses array of `film_details` for efficient film storage
- Implements quality classification based on recent performance
- Tracks activity status

#### actors_history_scd Table
Historical tracking table implementing SCD Type 2:
- Maintains complete history of changes
- Tracks effective dates through `start_date` and `end_date`
- Preserves quality classifications and activity status over time
- Enables point-in-time analysis

### 3. Data Loading Strategy

The system implements a two-phase data loading approach:

#### Initial Data Load
- Processes first year data
- Establishes baseline records
- Calculates initial quality classifications
- Example usage in Task 2 (Part 1)

#### Incremental Updates
- Processes subsequent years
- Maintains cumulative film history
- Updates classifications based on recent performance
- Example usage in Task 2 (Part 2)

### 4. SCD Implementation

#### Backfill Strategy (Task 4)
The backfill process:
1. Identifies changes in actor status or classification
2. Groups continuous periods (streaks)
3. Creates historical records with appropriate date ranges
4. Maintains referential integrity

#### Incremental Update Strategy (Task 5)
The incremental process handles:
1. Unchanged records (extends current period)
2. Changed records (closes current period, opens new)
3. New records (creates new entries)
4. Historical preservation (maintains past records)

## Practical Applications

### Query Types and Their Uses

#### 1. Classification Queries
```sql
CASE
    WHEN avg_year_rating > 8 THEN 'star'
    WHEN avg_year_rating > 7 THEN 'good'
    WHEN avg_year_rating > 6 THEN 'average'
    ELSE 'bad'
END::quality_class
```
Used for:
- Real-time actor performance evaluation
- Trend analysis
- Quality tracking over time

#### 2. Historical Tracking Queries
```sql
WITH streak_started AS (
    SELECT 
        ...,
        LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) <> quality_class
        OR LAG(is_active, 1) OVER (PARTITION BY actorid ORDER BY current_year) <> is_active
        AS did_change
    FROM actors
)
```
Used for:
- Change detection
- Period analysis
- Temporal consistency maintenance

#### 3. Data Integration Queries
```sql
CASE
    WHEN y.films IS NULL THEN
        ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::film_details]
    WHEN t.film IS NOT NULL THEN 
        y.films || ARRAY[ROW(t.film, t.votes, t.rating, t.filmid)::film_details]
    ELSE y.films
END as films
```
Used for:
- Data merging
- Cumulative history building
- State transitions

## Technical Considerations

### 1. Performance Optimization
- Uses array aggregation for film storage
- Implements efficient indexing through composite primary keys
- Minimizes data duplication through normalized design

### 2. Data Integrity
- Enforces type safety through custom types
- Maintains temporal consistency through SCD
- Preserves historical accuracy through careful state tracking

### 3. Maintainability
- Modular query design
- Clear separation of concerns
- Well-documented transformation logic

## Common Use Cases

### 1. Historical Analysis
```sql
SELECT * FROM actors_history_scd
WHERE start_date <= target_year 
AND end_date >= target_year;
```
- Point-in-time reporting
- Career trajectory analysis
- Performance trending

### 2. Quality Tracking
```sql
SELECT 
    actorid,
    actor_name,
    quality_class,
    start_date,
    end_date
FROM actors_history_scd
WHERE actorid = target_actor
ORDER BY start_date;
```
- Career progression tracking
- Performance period analysis
- Quality transition studies

### 3. Activity Monitoring
```sql
SELECT 
    COUNT(*) as active_count,
    quality_class
FROM actors
WHERE is_active = true
AND current_year = target_year
GROUP BY quality_class;
```
- Active actor tracking
- Quality distribution analysis
- Industry trend monitoring