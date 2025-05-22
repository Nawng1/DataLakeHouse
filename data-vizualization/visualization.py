import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from folium.plugins import HeatMap, MarkerCluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, min as spark_min, avg as spark_avg
import json
import numpy as np

# Directory with prepared ML datasets
INPUT_DIR = "ml_prepared_data"
# Directory to save visualizations
OUTPUT_DIR = "visualizations"

def create_spark_session():
    """Create a Spark session for data visualization"""
    spark = SparkSession.builder \
        .appName("FloodDataVisualization") \
        .getOrCreate()
    
    print("Spark session created for visualizations")
    return spark

def load_datasets(spark):
    """Load all prepared datasets for visualization"""
    datasets = {}
    
    # Check if the directory exists
    if not os.path.exists(INPUT_DIR):
        print(f"Input directory not found: {INPUT_DIR}")
        return datasets
    
    # Load stations data
    stations_path = os.path.join(INPUT_DIR, "prepared_stations")
    if os.path.exists(stations_path):
        datasets["stations"] = spark.read.parquet(stations_path)
        print(f"Loaded stations dataset: {datasets['stations'].count()} records")
    
    # Load readings data
    readings_path = os.path.join(INPUT_DIR, "prepared_readings")
    if os.path.exists(readings_path):
        datasets["readings"] = spark.read.parquet(readings_path)
        print(f"Loaded readings dataset: {datasets['readings'].count()} records")
    
    # Load historical data
    historical_path = os.path.join(INPUT_DIR, "prepared_historical")
    if os.path.exists(historical_path):
        datasets["historical"] = spark.read.parquet(historical_path)
        print(f"Loaded historical dataset: {datasets['historical'].count()} records")
    
    # Load warnings data
    warnings_path = os.path.join(INPUT_DIR, "prepared_warnings")
    if os.path.exists(warnings_path):
        datasets["warnings"] = spark.read.parquet(warnings_path)
        print(f"Loaded warnings dataset: {datasets['warnings'].count()} records")
    
    return datasets

def ensure_output_dir():
    """Ensure output directory exists"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory: {OUTPUT_DIR}")

def create_stations_map(datasets):
    """Create an interactive map of monitoring stations with current readings"""
    print("\n--- Creating interactive stations map ---")
    
    if "stations" not in datasets:
        print("Stations dataset not available")
        return
    
    # Convert to Pandas for visualization
    stations_pd = datasets["stations"].limit(1000).toPandas()
    
    # Filter out rows with missing coordinates
    stations_pd = stations_pd[stations_pd['latitude'].notna() & stations_pd['longitude'].notna()]
    
    if len(stations_pd) == 0:
        print("No valid station coordinates found")
        return
    
    # Create base map centered on average coordinates
    center_lat = stations_pd['latitude'].mean()
    center_lon = stations_pd['longitude'].mean()
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=6)
    
    # Add station markers
    marker_cluster = MarkerCluster().add_to(m)
    
    for _, station in stations_pd.iterrows():
        if pd.notna(station['latitude']) and pd.notna(station['longitude']):
            popup_text = f"""
                <b>{station['name']}</b><br>
                River: {station['river_name'] if pd.notna(station['river_name']) else 'N/A'}<br>
                Town: {station['town'] if pd.notna(station['town']) else 'N/A'}<br>
                Station ID: {station['station_id']}
            """
            folium.Marker(
                location=[station['latitude'], station['longitude']],
                popup=folium.Popup(popup_text, max_width=300),
                icon=folium.Icon(color='blue', icon='info-sign')
            ).add_to(marker_cluster)
    
    # Save the map
    output_file = os.path.join(OUTPUT_DIR, "stations_map.html")
    m.save(output_file)
    print(f"Interactive stations map saved to {output_file}")

def create_water_levels_heatmap(datasets):
    """Create a heatmap of water levels across stations"""
    print("\n--- Creating water levels heatmap ---")
    
    if "readings" not in datasets or "stations" not in datasets:
        print("Required datasets not available")
        return
    
    # Join readings with stations to get location data
    if datasets["readings"].count() > 0 and datasets["stations"].count() > 0:
        joined_df = datasets["readings"].join(
            datasets["stations"], 
            datasets["readings"].station_id == datasets["stations"].station_id,
            "inner"
        )
        
        # Select relevant columns and convert to Pandas
        heatmap_data = joined_df.select(
            col("latitude"), 
            col("longitude"), 
            col("value").alias("water_level")
        ).filter(
            col("latitude").isNotNull() & 
            col("longitude").isNotNull() & 
            col("water_level").isNotNull()
        ).toPandas()
        
        if len(heatmap_data) == 0:
            print("No valid heatmap data found")
            return
        
        # Create base map
        center_lat = heatmap_data['latitude'].mean()
        center_lon = heatmap_data['longitude'].mean()
        m = folium.Map(location=[center_lat, center_lon], zoom_start=7)
        
        # Normalize water levels for heatmap intensity
        heatmap_data['water_level_norm'] = (heatmap_data['water_level'] - heatmap_data['water_level'].min()) / \
                                         (heatmap_data['water_level'].max() - heatmap_data['water_level'].min())
        
        # Create heatmap data
        heat_data = [[row['latitude'], row['longitude'], row['water_level_norm']] 
                     for _, row in heatmap_data.iterrows()]
        
        # Add heatmap layer
        HeatMap(heat_data, radius=15, max_zoom=13, gradient={0.4: 'blue', 0.65: 'lime', 0.8: 'orange', 1: 'red'}).add_to(m)
        
        # Save the map
        output_file = os.path.join(OUTPUT_DIR, "water_levels_heatmap.html")
        m.save(output_file)
        print(f"Water levels heatmap saved to {output_file}")
    else:
        print("Insufficient data for heatmap generation")

def create_time_series_plots(datasets):
    """Create time series plots of water levels for selected stations"""
    print("\n--- Creating time series plots ---")
    
    if "historical" not in datasets:
        print("Historical dataset not available")
        return
    
    # Get top 5 stations with most data
    top_stations = datasets["historical"].groupBy("station_id") \
                                        .count() \
                                        .orderBy(col("count").desc()) \
                                        .limit(5) \
                                        .collect()
    
    station_ids = [row["station_id"] for row in top_stations]
    
    if not station_ids:
        print("No stations found for time series plots")
        return
    
    # Filter historical data for these stations
    historical_filtered = datasets["historical"].filter(col("station_id").isin(station_ids))
    
    # Convert to pandas for plotting
    df_pandas = historical_filtered.select("timestamp", "value", "station_id") \
                                  .orderBy("timestamp") \
                                  .toPandas()
    
    # Create time series plot
    plt.figure(figsize=(12, 8))
    
    for i, station_id in enumerate(station_ids):
        station_data = df_pandas[df_pandas["station_id"] == station_id]
        if len(station_data) > 0:
            plt.subplot(len(station_ids), 1, i+1)
            plt.plot(station_data["timestamp"], station_data["value"])
            plt.title(f"Station ID: {station_id}")
            plt.ylabel("Water Level")
            plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    output_file = os.path.join(OUTPUT_DIR, "water_levels_time_series.png")
    plt.savefig(output_file)
    plt.close()
    print(f"Time series plots saved to {output_file}")

def create_flood_warnings_plot(datasets):
    """Create a visualization of flood warnings by severity and area"""
    print("\n--- Creating flood warnings visualization ---")
    
    if "warnings" not in datasets:
        print("Warnings dataset not available")
        return
    
    # Get count of warnings by severity and area
    warnings_df = datasets["warnings"]
    
    if warnings_df.count() == 0:
        print("No warning data available")
        return
    
    # Count warnings by severity
    severity_counts = warnings_df.groupBy("severity") \
                                .count() \
                                .orderBy("severity") \
                                .toPandas()
    
    # Count warnings by area
    area_counts = warnings_df.groupBy("area_name") \
                            .count() \
                            .orderBy(col("count").desc()) \
                            .limit(10) \
                            .toPandas()
    
    # Create visualizations
    plt.figure(figsize=(15, 10))
    
    # Severity counts plot
    plt.subplot(2, 1, 1)
    bars = plt.bar(severity_counts["severity"], severity_counts["count"], color=['green', 'orange', 'red', 'gray'])
    plt.title("Flood Warnings by Severity")
    plt.xlabel("Severity")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    
    # Add count labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom')
    
    # Area counts plot
    plt.subplot(2, 1, 2)
    bars = plt.barh(area_counts["area_name"], area_counts["count"], color='steelblue')
    plt.title("Top 10 Areas with Most Flood Warnings")
    plt.xlabel("Count")
    plt.ylabel("Area Name")
    
    # Add count labels at end of bars
    for bar in bars:
        width = bar.get_width()
        plt.text(width, bar.get_y() + bar.get_height()/2.,
                f'{int(width)}', ha='left', va='center')
    
    plt.tight_layout()
    
    # Save the plot
    output_file = os.path.join(OUTPUT_DIR, "flood_warnings_analysis.png")
    plt.savefig(output_file)
    plt.close()
    print(f"Flood warnings visualization saved to {output_file}")

def create_correlation_heatmap(datasets):
    """Create a correlation heatmap for water levels and time features"""
    print("\n--- Creating correlation heatmap ---")
    
    if "historical" not in datasets:
        print("Historical dataset not available")
        return
    
    # Select station with most data
    top_station = datasets["historical"].groupBy("station_id") \
                                      .count() \
                                      .orderBy(col("count").desc()) \
                                      .limit(1) \
                                      .collect()
    
    if not top_station:
        print("No stations found for correlation analysis")
        return
    
    station_id = top_station[0]["station_id"]
    
    # Filter data for this station and select relevant columns
    station_data = datasets["historical"].filter(col("station_id") == station_id) \
                                       .select("value", "hour", "dayofweek", "day", "month") \
                                       .toPandas()
    
    if len(station_data) == 0:
        print("No data available for correlation analysis")
        return
    
    # Create correlation matrix
    corr_matrix = station_data.corr()
    
    # Create heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', vmin=-1, vmax=1, center=0)
    plt.title(f"Correlation Heatmap for Station {station_id}")
    
    # Save the plot
    output_file = os.path.join(OUTPUT_DIR, "correlation_heatmap.png")
    plt.savefig(output_file)
    plt.close()
    print(f"Correlation heatmap saved to {output_file}")

def create_flood_risk_map(datasets):
    """Create a map of flood risk areas based on warnings and water levels"""
    print("\n--- Creating flood risk map ---")
    
    if "warnings" not in datasets or "stations" not in datasets:
        print("Required datasets not available")
        return
    
    warnings_df = datasets["warnings"]
    stations_df = datasets["stations"]
    
    if warnings_df.count() == 0 or stations_df.count() == 0:
        print("Insufficient data for flood risk map")
        return
    
    # Create a map
    uk_center = [52.561928, -1.464854]  # Approximate center of the UK
    m = folium.Map(location=uk_center, zoom_start=6)
    
    # Add markers for warnings
    warnings_pd = warnings_df.select(
        "county", 
        "severity", 
        "severity_level",
        "description"
    ).toPandas()
    
    # Add markers for stations with high water levels
    if "readings" in datasets:
        # Join readings with stations
        high_levels = datasets["readings"].join(
            datasets["stations"],
            datasets["readings"].station_id == datasets["stations"].station_id,
            "inner"
        ).select(
            datasets["stations"].latitude,
            datasets["stations"].longitude,
            datasets["readings"].value,
            datasets["stations"].name
        ).filter(
            col("latitude").isNotNull() & 
            col("longitude").isNotNull()
        )
        
        # Calculate percentiles for water levels
        quantiles = high_levels.approxQuantile("value", [0.9], 0.1)
        if quantiles and len(quantiles) > 0:
            high_threshold = quantiles[0]
            
            # Filter for high water levels
            high_levels = high_levels.filter(col("value") >= high_threshold).toPandas()
            
            # Add markers for high water levels
            for _, row in high_levels.iterrows():
                folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=6,
                    popup=f"Station: {row['name']}<br>Water Level: {row['value']:.2f}",
                    color='blue',
                    fill=True,
                    fill_color='blue',
                    fill_opacity=0.6
                ).add_to(m)
    
    # Save the map
    output_file = os.path.join(OUTPUT_DIR, "flood_risk_map.html")
    m.save(output_file)
    print(f"Flood risk map saved to {output_file}")

def create_dashboard_summary_stats(datasets):
    """Create summary statistics for a dashboard"""
    print("\n--- Creating dashboard summary statistics ---")
    
    summary_stats = {}
    
    # Count total stations
    if "stations" in datasets:
        summary_stats["total_stations"] = datasets["stations"].count()
    
    # Count active warnings
    if "warnings" in datasets:
        summary_stats["total_warnings"] = datasets["warnings"].count()
        
        # Count by severity
        severity_counts = datasets["warnings"].groupBy("severity") \
                                             .count() \
                                             .collect()
        
        severity_dict = {row["severity"]: row["count"] for row in severity_counts}
        summary_stats["warnings_by_severity"] = severity_dict
    
    # Get water level statistics
    if "readings" in datasets:
        readings_stats = datasets["readings"].agg(
            spark_min("value").alias("min_level"),
            spark_avg("value").alias("avg_level"),
            spark_max("value").alias("max_level")
        ).collect()[0]
        
        summary_stats["water_level_stats"] = {
            "min": readings_stats["min_level"],
            "avg": readings_stats["avg_level"],
            "max": readings_stats["max_level"]
        }
    
    # Save summary stats as JSON
    output_file = os.path.join(OUTPUT_DIR, "dashboard_summary.json")
    with open(output_file, 'w') as f:
        json.dump(summary_stats, f, indent=2)
    
    print(f"Dashboard summary statistics saved to {output_file}")

def main():
    print("Flood Data Visualization Generator")
    print("=================================")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Ensure output directory exists
    ensure_output_dir()
    
    # Load datasets
    datasets = load_datasets(spark)
    
    if not datasets:
        print("No datasets available for visualization")
        return
    
    # Create visualizations
    create_stations_map(datasets)
    create_water_levels_heatmap(datasets)
    create_time_series_plots(datasets)
    create_flood_warnings_plot(datasets)
    create_correlation_heatmap(datasets)
    create_flood_risk_map(datasets)
    create_dashboard_summary_stats(datasets)
    
    print("\nAll visualizations generated successfully!")
    print(f"Visualizations saved to {OUTPUT_DIR}")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main() 