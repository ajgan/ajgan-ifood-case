from pyspark import pipelines as dp

# Escolhi fazer uma tabela Source of Record(bronze) para cada arquivo de entrada. 
# Essa ideia é pensando num caso de mundo real em que todo mês teríamos um novo arquivo chegando. 
# Em casos desse novo arquivo ter algum layout defeituoso, sua carga não funcionaria se fosse feita diretamente em uma tabela com todas as informações agrupadas. 
# Se a gente coloca cada arquivo em uma tabela especifica para ele, conseguimos isolar os problemas e teremos uma tabela funcional independente da qualidade dos dados de entrada, o que permite exploração desses dados para entendimento de um possível problema detectado na camada posterior.

# O código feito está "engessado" para os 5 meses propostos, mas a lógica é facilmente adaptável para receber mais meses, podendo fazer uso de variáveis de código para termos um pipeline produtivo parametrizável.


# =============================================================================
# YELLOW CAB BRONZE TABLES (Jan-May 2023)
# =============================================================================

@dp.materialized_view(
    comment="Yellow cab trips for January 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_yellow_2023_01():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/yellow/yellow_tripdata_2023-01.parquet")

@dp.materialized_view(
    comment="Yellow cab trips for February 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_yellow_2023_02():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/yellow/yellow_tripdata_2023-02.parquet")

@dp.materialized_view(
    comment="Yellow cab trips for March 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_yellow_2023_03():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/yellow/yellow_tripdata_2023-03.parquet")

@dp.materialized_view(
    comment="Yellow cab trips for April 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_yellow_2023_04():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/yellow/yellow_tripdata_2023-04.parquet")

@dp.materialized_view(
    comment="Yellow cab trips for May 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_yellow_2023_05():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/yellow/yellow_tripdata_2023-05.parquet")


# =============================================================================
# GREEN CAB BRONZE TABLES (Jan-May 2023)
# =============================================================================

@dp.materialized_view(
    comment="Green cab trips for January 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_green_2023_01():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/green/green_tripdata_2023-01.parquet")

@dp.materialized_view(
    comment="Green cab trips for February 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_green_2023_02():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/green/green_tripdata_2023-02.parquet")

@dp.materialized_view(
    comment="Green cab trips for March 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_green_2023_03():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/green/green_tripdata_2023-03.parquet")

@dp.materialized_view(
    comment="Green cab trips for April 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_green_2023_04():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/green/green_tripdata_2023-04.parquet")

@dp.materialized_view(
    comment="Green cab trips for May 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_green_2023_05():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/green/green_tripdata_2023-05.parquet")


# =============================================================================
# HVFHV BRONZE TABLES (Jan-May 2023)
# =============================================================================

@dp.materialized_view(
    comment="HVFHV trips for January 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_hvfhv_2023_01():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/hvfhv/fhvhv_tripdata_2023-01.parquet")

@dp.materialized_view(
    comment="HVFHV trips for February 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_hvfhv_2023_02():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/hvfhv/fhvhv_tripdata_2023-02.parquet")

@dp.materialized_view(
    comment="HVFHV trips for March 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_hvfhv_2023_03():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/hvfhv/fhvhv_tripdata_2023-03.parquet")

@dp.materialized_view(
    comment="HVFHV trips for April 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_hvfhv_2023_04():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/hvfhv/fhvhv_tripdata_2023-04.parquet")

@dp.materialized_view(
    comment="HVFHV trips for May 2023",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def bronze_hvfhv_2023_05():
    return spark.read.format("parquet").load("/Volumes/workspace/default/taxi_data_source/hvfhv/fhvhv_tripdata_2023-05.parquet")
