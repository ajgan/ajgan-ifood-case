from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Para a segunda camada de Source of Truth(silver), deicidi fazer uso de uma tabela unificada com todas as informações provenientes da camada bronze.
# Em um pipeline produtivo, toda tabela bronze carregada seria appendada na tabela unificada silver para uma visualização completa dos dados.
# Aqui, vale destacar que selecionei apenas as colunas listadas como obrigatórias, além de adicionar mais duas que fizeram sentido(tipo de táxi e mês da corrida)

# Essa camada já pode ser consumida e fiz as análises das perguntas em cima dela. Mas ainda podemos criar a camada gold da arquitetura medalhão com informações agregadas.
# Não fiz camada gold porque entendo a gold como algo mais especializado e que depende do uso de consumo. Podemos agregar por VendorID, por tipo de táxi, por região, etc. 
# E a partir da dimensão escolhida, podemos ter informações como soma, média, valor minimo, valor maximo, etc, informações que podem ser úteis para uso em modelos estatísticos e consumo rápido, mas que para esse projeto não senti necessidade 

# Fiz o projeto pelo Databricks Community Edition. Mas só para agregar no esboço, tudo pode ser feito em ambiente AWS. 
# Uma ideia básica seria utilizar um Glue com o Spark para fazer a carga dos arquivos parquet num bucket bronze no S3. 
# Depois podemos ter outro job que carrega num bucket camada silver, separando em partições com ano-mes.
# Tudo pode ser orquestrado por Step Functions e salvando logs no CloudWatch
# Podemos criar uma tabela no Athena e o consumo pode ser feito por lá(dentre outras opções)

# O código feito está "engessado" para os 5 meses propostos, mas a lógica é facilmente adaptável para receber mais meses, podendo fazer uso de variáveis de código para termos um pipeline produtivo parametrizável.

# Silver layer: Unified trips with standardized columns and trip_category
@dp.materialized_view(
    comment="Unified trip data from yellow, green, and HVFHV sources with standardized columns and source month",
    cluster_by=["trip_category", "source_month"],
    table_properties={"delta.enableChangeDataFeed": "true", "delta.feature.timestampNtz": "supported"}
)
def silver_unified_trips():
    # Read all yellow bronze tables (Jan-May 2023)
    yellow_tables = []
    for month in ["01", "02", "03", "04", "05"]:
        df = spark.read.table(f"bronze_yellow_2023_{month}").select(
            F.col("VendorID"),
            F.col("passenger_count"),
            F.col("total_amount"),
            F.col("tpep_pickup_datetime"),
            F.col("tpep_dropoff_datetime"),
            F.lit("yellow").alias("trip_category"),
            F.lit(f"2023-{month}").alias("source_month")
        )
        yellow_tables.append(df)
    
    # Read all green bronze tables (Jan-May 2023)
    green_tables = []
    for month in ["01", "02", "03", "04", "05"]:
        df = spark.read.table(f"bronze_green_2023_{month}").select(
            F.col("VendorID"),
            F.col("passenger_count"),
            F.col("total_amount"),
            F.col("lpep_pickup_datetime").alias("tpep_pickup_datetime"),
            F.col("lpep_dropoff_datetime").alias("tpep_dropoff_datetime"),
            F.lit("green").alias("trip_category"),
            F.lit(f"2023-{month}").alias("source_month")
        )
        green_tables.append(df)
    
    # Read all hvfhv bronze tables (Jan-May 2023)
    hvfhv_tables = []
    for month in ["01", "02", "03", "04", "05"]:
        df = spark.read.table(f"bronze_hvfhv_2023_{month}").select(
            F.lit(None).cast("bigint").alias("VendorID"),
            F.lit(None).cast("double").alias("passenger_count"),
            F.col("base_passenger_fare").alias("total_amount"),
            F.col("pickup_datetime").alias("tpep_pickup_datetime"),
            F.col("dropoff_datetime").alias("tpep_dropoff_datetime"),
            F.lit("hvfhv").alias("trip_category"),
            F.lit(f"2023-{month}").alias("source_month")
        )
        hvfhv_tables.append(df)
    
    # Combine all tables
    all_tables = yellow_tables + green_tables + hvfhv_tables
    
    # Union all 15 bronze tables
    result = all_tables[0]
    for df in all_tables[1:]:
        result = result.union(df)
    
    return result
