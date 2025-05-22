-- Trino Queries for Flood Monitoring Data Visualization
-- Connect to Trino at localhost:8086 and run these queries

-- 1. Show all available databases and tables
SHOW CATALOGS;
SHOW SCHEMAS FROM hive;
SHOW TABLES FROM hive.flood_monitoring;

-- 2. Station Overview - Geographic distribution
SELECT 
    station_type,
    COUNT(*) as station_count,
    AVG(latitude) as avg_latitude,
    AVG(longitude) as avg_longitude
FROM hive.flood_monitoring.stations 
GROUP BY station_type
ORDER BY station_count DESC;

-- 3. Water Level Analysis - Current status by region
SELECT 
    s.river_name,
    s.town,
    COUNT(DISTINCT s.station_id) as station_count,
    AVG(wl.water_level) as avg_water_level,
    MAX(wl.water_level) as max_water_level,
    MIN(wl.water_level) as min_water_level
FROM hive.flood_monitoring.stations s
LEFT JOIN hive.flood_monitoring.water_levels wl ON s.station_id = wl.station_id
WHERE s.river_name IS NOT NULL
GROUP BY s.river_name, s.town
ORDER BY avg_water_level DESC NULLS LAST
LIMIT 20;

-- 4. Flood Warning Analysis - Severity distribution
SELECT 
    severity,
    severity_level,
    COUNT(*) as warning_count,
    AVG(warning_duration_hours) as avg_duration_hours
FROM hive.flood_monitoring.flood_warnings
GROUP BY severity, severity_level
ORDER BY severity_level;

-- 5. Time Series Analysis - Water level trends
SELECT 
    DATE_TRUNC('day', reading_datetime) as reading_date,
    station_id,
    AVG(water_level) as daily_avg_level,
    MAX(water_level) as daily_max_level,
    MIN(water_level) as daily_min_level,
    STDDEV(water_level) as daily_std_level
FROM hive.flood_monitoring.historical_readings
WHERE reading_datetime >= DATE '2024-01-01'
GROUP BY DATE_TRUNC('day', reading_datetime), station_id
ORDER BY reading_date DESC, daily_avg_level DESC
LIMIT 100;

-- 6. Risk Assessment - High-risk stations
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
    AVG(rolling_avg_3h) as avg_rolling_3h
FROM hive.flood_monitoring.ml_features
WHERE risk_level = 'HIGH'
GROUP BY station_id, station_name, latitude, longitude, river_name, town, risk_level
ORDER BY risk_occurrence_count DESC
LIMIT 20;

-- 7. Geographic Analysis - Stations by county with warning counts
SELECT 
    fa.county,
    COUNT(DISTINCT s.station_id) as station_count,
    COUNT(DISTINCT fw.warning_id) as warning_count,
    AVG(s.latitude) as avg_latitude,
    AVG(s.longitude) as avg_longitude
FROM hive.flood_monitoring.flood_areas fa
LEFT JOIN hive.flood_monitoring.flood_warnings fw ON fa.area_id = fw.flood_area_id
LEFT JOIN hive.flood_monitoring.stations s ON fa.river_or_sea = s.river_name
WHERE fa.county IS NOT NULL
GROUP BY fa.county
ORDER BY warning_count DESC, station_count DESC;

-- 8. ML Features Summary - For model training
SELECT 
    station_type,
    risk_level,
    COUNT(*) as record_count,
    AVG(water_level) as avg_water_level,
    AVG(level_change) as avg_level_change,
    AVG(rolling_avg_3h) as avg_rolling_3h,
    AVG(rolling_std_3h) as avg_rolling_std_3h
FROM hive.flood_monitoring.ml_features mf
JOIN hive.flood_monitoring.stations s ON mf.station_id = s.station_id
GROUP BY station_type, risk_level
ORDER BY station_type, risk_level;

-- 9. Hourly Pattern Analysis - For seasonal modeling
SELECT 
    hour,
    month,
    AVG(water_level) as avg_water_level,
    COUNT(*) as reading_count
FROM hive.flood_monitoring.historical_readings
GROUP BY hour, month
ORDER BY month, hour;

-- 10. Real-time Dashboard Query - Current status overview
SELECT 
    'Total Stations' as metric,
    CAST(COUNT(DISTINCT station_id) AS VARCHAR) as value
FROM hive.flood_monitoring.stations
UNION ALL
SELECT 
    'Active Warnings',
    CAST(COUNT(*) AS VARCHAR)
FROM hive.flood_monitoring.flood_warnings
UNION ALL
SELECT 
    'High Risk Stations',
    CAST(COUNT(DISTINCT station_id) AS VARCHAR)
FROM hive.flood_monitoring.ml_features
WHERE risk_level = 'HIGH'
UNION ALL
SELECT 
    'Latest Reading Time',
    CAST(MAX(reading_datetime) AS VARCHAR)
FROM hive.flood_monitoring.water_levels; 