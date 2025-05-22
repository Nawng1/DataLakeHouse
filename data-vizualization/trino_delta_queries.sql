-- Trino Queries for Delta Lake Tables in Flood Monitoring Data
-- Connect to Trino at localhost:8086 and run these queries

-- 1. Show all Delta tables
SHOW TABLES FROM hive.flood_monitoring LIKE '%delta%';

-- 2. Station Overview - Delta table with processing timestamp
SELECT 
    station_type,
    COUNT(*) as station_count,
    AVG(latitude) as avg_latitude,
    AVG(longitude) as avg_longitude,
    MAX(processed_at) as last_processed
FROM hive.flood_monitoring.stations_delta 
GROUP BY station_type
ORDER BY station_count DESC;

-- 3. Water Level Analysis with Delta versioning capabilities
SELECT 
    s.river_name,
    s.town,
    COUNT(DISTINCT s.station_id) as station_count,
    AVG(wl.water_level) as avg_water_level,
    MAX(wl.water_level) as max_water_level,
    MIN(wl.water_level) as min_water_level,
    MAX(wl.processed_at) as last_updated
FROM hive.flood_monitoring.stations_delta s
LEFT JOIN hive.flood_monitoring.water_levels_delta wl ON s.station_id = wl.station_id
WHERE s.river_name IS NOT NULL
GROUP BY s.river_name, s.town
ORDER BY avg_water_level DESC NULLS LAST
LIMIT 20;

-- 4. Time Series Analysis with Delta Lake features
SELECT 
    DATE_TRUNC('day', reading_datetime) as reading_date,
    station_id,
    AVG(water_level) as daily_avg_level,
    AVG(rolling_avg_3h) as daily_avg_rolling_3h,
    MAX(water_level) as daily_max_level,
    MIN(water_level) as daily_min_level,
    STDDEV(water_level) as daily_std_level,
    COUNT(*) as reading_count
FROM hive.flood_monitoring.historical_readings_delta
WHERE reading_datetime >= DATE '2024-01-01'
GROUP BY DATE_TRUNC('day', reading_datetime), station_id
ORDER BY reading_date DESC, daily_avg_level DESC
LIMIT 100;

-- 5. ML Features from Delta table - Risk Assessment
SELECT 
    station_id,
    station_name,
    latitude,
    longitude,
    river_name,
    town,
    risk_level,
    COUNT(*) as risk_occurrence_count,
    AVG(water_level) as avg_risk_level,
    AVG(rolling_avg_3h) as avg_rolling_3h,
    AVG(level_change) as avg_level_change,
    MAX(processed_at) as last_updated
FROM hive.flood_monitoring.ml_features_delta
WHERE risk_level = 'HIGH'
GROUP BY station_id, station_name, latitude, longitude, river_name, town, risk_level
ORDER BY risk_occurrence_count DESC
LIMIT 20;

-- 6. Flood Warning Analysis with Delta processing info
SELECT 
    severity,
    severity_level,
    COUNT(*) as warning_count,
    AVG(warning_duration_hours) as avg_duration_hours,
    MIN(time_raised) as earliest_warning,
    MAX(time_raised) as latest_warning,
    MAX(processed_at) as last_processed
FROM hive.flood_monitoring.flood_warnings_delta
GROUP BY severity, severity_level
ORDER BY severity_level;

-- 7. Geographic Analysis with Delta Lake data freshness
SELECT 
    fa.county,
    COUNT(DISTINCT s.station_id) as station_count,
    COUNT(DISTINCT fw.warning_id) as warning_count,
    AVG(s.latitude) as avg_latitude,
    AVG(s.longitude) as avg_longitude,
    MAX(s.processed_at) as stations_last_updated,
    MAX(fw.processed_at) as warnings_last_updated
FROM hive.flood_monitoring.flood_areas_delta fa
LEFT JOIN hive.flood_monitoring.flood_warnings_delta fw ON fa.area_id = fw.flood_area_id
LEFT JOIN hive.flood_monitoring.stations_delta s ON fa.river_or_sea = s.river_name
WHERE fa.county IS NOT NULL
GROUP BY fa.county
ORDER BY warning_count DESC, station_count DESC;

-- 8. Data Quality Check - Processing timestamps across Delta tables
SELECT 
    'stations_delta' as table_name,
    COUNT(*) as record_count,
    MIN(processed_at) as first_processed,
    MAX(processed_at) as last_processed
FROM hive.flood_monitoring.stations_delta
UNION ALL
SELECT 
    'water_levels_delta',
    COUNT(*),
    MIN(processed_at),
    MAX(processed_at)
FROM hive.flood_monitoring.water_levels_delta
UNION ALL
SELECT 
    'historical_readings_delta',
    COUNT(*),
    MIN(processed_at),
    MAX(processed_at)
FROM hive.flood_monitoring.historical_readings_delta
UNION ALL
SELECT 
    'flood_warnings_delta',
    COUNT(*),
    MIN(processed_at),
    MAX(processed_at)
FROM hive.flood_monitoring.flood_warnings_delta
UNION ALL
SELECT 
    'flood_areas_delta',
    COUNT(*),
    MIN(processed_at),
    MAX(processed_at)
FROM hive.flood_monitoring.flood_areas_delta;

-- 9. ML Features Summary with Delta Lake advantages
SELECT 
    station_type,
    risk_level,
    COUNT(*) as record_count,
    AVG(water_level) as avg_water_level,
    AVG(level_change) as avg_level_change,
    AVG(rolling_avg_3h) as avg_rolling_3h,
    AVG(rolling_std_3h) as avg_rolling_std_3h,
    MAX(processed_at) as last_updated
FROM hive.flood_monitoring.ml_features_delta mf
JOIN hive.flood_monitoring.stations_delta s ON mf.station_id = s.station_id
GROUP BY station_type, risk_level
ORDER BY station_type, risk_level;

-- 10. Real-time Dashboard with Delta Lake data lineage
SELECT 
    'Total Stations (Delta)' as metric,
    CAST(COUNT(DISTINCT station_id) AS VARCHAR) as value,
    CAST(MAX(processed_at) AS VARCHAR) as last_updated
FROM hive.flood_monitoring.stations_delta
UNION ALL
SELECT 
    'Active Warnings (Delta)',
    CAST(COUNT(*) AS VARCHAR),
    CAST(MAX(processed_at) AS VARCHAR)
FROM hive.flood_monitoring.flood_warnings_delta
UNION ALL
SELECT 
    'High Risk Stations (Delta)',
    CAST(COUNT(DISTINCT station_id) AS VARCHAR),
    CAST(MAX(processed_at) AS VARCHAR)
FROM hive.flood_monitoring.ml_features_delta
WHERE risk_level = 'HIGH'
UNION ALL
SELECT 
    'Latest Reading Time (Delta)',
    CAST(MAX(reading_datetime) AS VARCHAR),
    CAST(MAX(processed_at) AS VARCHAR)
FROM hive.flood_monitoring.water_levels_delta;

-- 11. Delta Lake specific features - Schema evolution check
-- (This would show if schema has evolved over time)
SELECT 
    COUNT(DISTINCT station_type) as unique_station_types,
    COUNT(DISTINCT CASE WHEN latitude IS NOT NULL THEN 1 END) as stations_with_coords,
    COUNT(DISTINCT CASE WHEN river_name IS NOT NULL THEN 1 END) as stations_with_river,
    MIN(processed_at) as first_processed,
    MAX(processed_at) as last_processed
FROM hive.flood_monitoring.stations_delta;

-- 12. Partition information for Delta tables
SELECT 
    'historical_readings_delta' as table_name,
    station_id,
    year,
    month,
    COUNT(*) as record_count,
    MIN(reading_datetime) as earliest_reading,
    MAX(reading_datetime) as latest_reading
FROM hive.flood_monitoring.historical_readings_delta
GROUP BY station_id, year, month
ORDER BY year DESC, month DESC, record_count DESC
LIMIT 50; 