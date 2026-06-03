- README e requirements gerados pelo Genie Code do Databricks Community Edition, mas revisados por mim.
- Código src também foi gerado com auxílio da IA para parte bruta, mas totalmente revisado, refinado e refeito por mim até ter resultado satisfatório
- Notebook de análise não teve auxílio de IA para construção
- Comentários sobre exploração dos dados e processo de decisão foram incluídos no começo de cada arquivo python da pasta src e nas células de markdown do notebook de análise.

# NYC Taxi Trip Data Pipeline

A Databricks Spark Declarative Pipeline (formerly Delta Live Tables) for processing NYC taxi trip data from multiple sources (Yellow, Green, and High Volume For-Hire Vehicle).

## Pipeline Architecture

This pipeline follows the **Medallion Architecture** pattern:

```
Bronze Layer (15 tables)
    ↓
Silver Layer (1 unified table)
    ↓
Gold Layer (no table created)
```

### Bronze Layer
- **15 materialized views** - One per source file (5 months × 3 taxi types)
- Raw data ingestion from Parquet files
- Tables: `bronze_yellow_2023_01` through `_05`, `bronze_green_2023_01` through `_05`, `bronze_hvfhv_2023_01` through `_05`
- Location: `/transformations/bronze/all_bronze_tables.py`

### Silver Layer
- **1 materialized view** - `silver_unified_trips`
- Unified and standardized trip data from all bronze sources
- Standardized columns across all taxi types
- Added columns:
  - `trip_category` (yellow/green/hvfhv)
  - `source_month` (yyyy-MM format, inferred from bronze table name)
- Clustered by: `trip_category`, `source_month`
- Location: `/transformations/silver/unified_trips.py`

### Gold Layer
- **dismissed for this project**

## Prerequisites

### 1. Data Sources
The pipeline expects Parquet files to be stored in Unity Catalog volumes:

```
/Volumes/workspace/default/taxi_data_source/
├── yellow/
│   ├── yellow_tripdata_2023-01.parquet
│   ├── yellow_tripdata_2023-02.parquet
│   ├── yellow_tripdata_2023-03.parquet
│   ├── yellow_tripdata_2023-04.parquet
│   └── yellow_tripdata_2023-05.parquet
├── green/
│   ├── green_tripdata_2023-01.parquet
│   ├── green_tripdata_2023-02.parquet
│   ├── green_tripdata_2023-03.parquet
│   ├── green_tripdata_2023-04.parquet
│   └── green_tripdata_2023-05.parquet
└── hvfhv/
    ├── fhvhv_tripdata_2023-01.parquet
    ├── fhvhv_tripdata_2023-02.parquet
    ├── fhvhv_tripdata_2023-03.parquet
    ├── fhvhv_tripdata_2023-04.parquet
    └── fhvhv_tripdata_2023-05.parquet
```

**Data source**: NYC TLC Trip Record Data - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### 2. Unity Catalog Setup
- **Catalog**: `workspace`
- **Schema**: `default`
- **Volume**: `taxi_data_source` (must exist in `workspace.default`)

### 3. Permissions Required
- `READ VOLUME` on `workspace.default.taxi_data_source`
- `WRITE VOLUME` on `workspace.default.taxi_data_source` (if uploading data)
- `CREATE TABLE` on schema `workspace.default`
- `USE CATALOG` on `workspace`
- `USE SCHEMA` on `workspace.default`

## Importing and Running the Pipeline

### Step 1: Upload Data to Volume

```python
# Create the volume if it doesn't exist
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.taxi_data_source")

# Upload your Parquet files using Databricks UI or CLI:
# 1. Navigate to Catalog > workspace > default > taxi_data_source
# 2. Create folders: yellow, green, hvfhv
# 3. Upload the corresponding Parquet files to each folder
```

Or using Databricks CLI:
```bash
# Upload files to volume
databricks fs cp ./data/yellow/ /Volumes/workspace/default/taxi_data_source/yellow/ --recursive
databricks fs cp ./data/green/ /Volumes/workspace/default/taxi_data_source/green/ --recursive
databricks fs cp ./data/hvfhv/ /Volumes/workspace/default/taxi_data_source/hvfhv/ --recursive
```

### Step 2: Import Source Files

```bash
# Using Databricks CLI
databricks workspace import_dir \
  ./nyc-taxi-pipeline \
  /Users/<YOUR_USERNAME>/nyc-taxi-pipeline \
  --overwrite
```

Or manually upload files via Databricks workspace UI:
1. Navigate to Workspace
2. Create folder: `/Users/<YOUR_USERNAME>/nyc-taxi-pipeline`
3. Create subfolder structure: `transformations/bronze/`, `transformations/silver/`, `transformations/gold/`
4. Upload the three Python files to their respective locations

### Step 3: Create the Pipeline

#### Option A: Using Databricks UI
1. Go to **Workflows** → **Pipelines**
2. Click **Create Pipeline**
3. Configure:
   - **Name**: NYC Taxi Trip Data Pipeline
   - **Product Edition**: Advanced (for serverless)
   - **Source code**: Add glob pattern: `/Users/<YOUR_USERNAME>/nyc-taxi-pipeline/transformations/**`
   - **Target catalog**: `workspace`
   - **Target schema**: `default`
   - **Cluster mode**: Serverless
   - **Photon**: Enabled
   - **Advanced configuration**: Add `spark.databricks.delta.properties.defaults.enableTimestampNtz = true`
4. Click **Create**

#### Option B: Using Databricks CLI/API

```bash
# Create pipeline using API
curl -X POST https://<databricks-instance>/api/2.0/pipelines \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "NYC Taxi Trip Data Pipeline",
    "catalog": "workspace",
    "target": "default",
    "continuous": false,
    "photon": true,
    "serverless": true,
    "channel": "CURRENT",
    "configuration": {
      "spark.databricks.delta.properties.defaults.enableTimestampNtz": "true"
    },
    "libraries": [
      {
        "glob": {
          "include": "/Workspace/Users/<YOUR_USERNAME>/nyc-taxi-pipeline/transformations/**"
        }
      }
    ]
  }'
```

### Step 4: Run the Pipeline

#### Via UI:
1. Open your pipeline
2. Click **Start** to run a full refresh

#### Via CLI:
```bash
# Start a pipeline update
databricks pipelines start-update <pipeline-id>

# Start a full refresh
databricks pipelines start-update <pipeline-id> --full-refresh
```

#### Via API:
```bash
curl -X POST https://<databricks-instance>/api/2.0/pipelines/<pipeline-id>/updates \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"full_refresh": false}'
```

## Pipeline Validation

After the pipeline runs successfully, validate the results:

```sql
-- Check bronze tables
SELECT COUNT(*) FROM workspace.default.bronze_yellow_2023_01;
SELECT COUNT(*) FROM workspace.default.bronze_green_2023_01;
SELECT COUNT(*) FROM workspace.default.bronze_hvfhv_2023_01;

-- Check silver table
SELECT 
    source_month,
    trip_category,
    COUNT(*) as trip_count
FROM workspace.default.silver_unified_trips
GROUP BY source_month, trip_category
ORDER BY source_month, trip_category;

```

Expected results:
- **15 bronze tables** with ~112M total rows
- **1 silver table** with 112M rows (all sources combined)

## Monitoring and Troubleshooting

### View Pipeline Status
```python
# Check pipeline status
display(spark.sql(f"""
    SELECT * FROM system.pipelines.pipeline_details 
    WHERE pipeline_id = '<your-pipeline-id>'
"""))

# Check latest update
display(spark.sql(f"""
    SELECT * FROM system.pipelines.pipeline_updates 
    WHERE pipeline_id = '<your-pipeline-id>'
    ORDER BY start_time DESC
    LIMIT 1
"""))
```

### Common Issues

1. **Permission Denied on Volume**
   ```sql
   -- Grant read access to volume
   GRANT READ VOLUME ON VOLUME workspace.default.taxi_data_source TO `<user-or-group>`;
   ```

2. **Table Feature Not Supported**
   - Ensure Databricks Runtime is 13.0+ (Serverless handles this automatically)
   - Check that `spark.databricks.delta.properties.defaults.enableTimestampNtz` is set to `true`

3. **Pipeline Validation Failed**
   - Review pipeline event logs in the UI
   - Check that all source files exist in the specified paths
   - Verify volume paths are correct

## Pipeline Configuration

### Key Settings
- **Serverless**: Enabled (auto-scaling, no cluster management)
- **Photon**: Enabled (accelerated query engine)
- **Continuous**: Disabled (triggered/scheduled runs)
- **Channel**: CURRENT (latest stable features)
- **Timestamp NTZ**: Enabled (no timezone, better performance)

### Table Features
All tables use:
- **Change Data Feed**: Enabled (for downstream CDC)
- **Timestamp NTZ**: Supported (efficient timestamp storage)
- **Liquid Clustering**: On silver and gold tables for query optimization

## Maintenance

### Adding New Months
To add new months of data:

1. Upload new Parquet files to the volume (e.g., `yellow_tripdata_2023-06.parquet`)
2. Update `transformations/bronze/all_bronze_tables.py`:
   ```python
   for month in ["01", "02", "03", "04", "05", "06"]:  # Add "06"
   ```
3. Run a pipeline update (not full refresh)

### Modifying Data Sources
If you need to change data sources or add new taxi types:
1. Update bronze layer to read from new sources
2. Update silver layer to include new sources in the union
3. Update gold layer if aggregation logic changes

## Support

For issues or questions:
- **Databricks Documentation**: https://docs.databricks.com/workflows/delta-live-tables/
- **Pipeline Settings Reference**: Check the pipeline settings file or API documentation
- **NYC TLC Data**: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Version History

- **v1.0** (2026-06-02): Initial pipeline with 15 bronze tables, 1 silver
- **v1.1** (2026-06-03): Added `source_month` column to silver table for better traceability

