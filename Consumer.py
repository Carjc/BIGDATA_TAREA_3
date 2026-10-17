# Librerias SPARK

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    from_json,
    col,
    avg
)

from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType
)

# Creación Session SPARK

spark = SparkSession.builder \
    .appName("TRMStreaming") \
    .getOrCreate()

# Reduce mensajes innecesarios del log
spark.sparkContext.setLogLevel("WARN")

# Define Esquema JSON

# Debe coincidir exactamente con la estructura enviada desde kafka_producer.py

schema = StructType([

    StructField(
        "valor",
        DoubleType(),
        True
    ),

    StructField(
        "unidad",
        StringType(),
        True
    ),

    StructField(
        "tipo_moneda",
        StringType(),
        True
    ),

    StructField(
        "vigencia_desde",
        StringType(),
        True
    ),

    StructField(
        "vigencia_hasta",
        StringType(),
        True
    )
])

# Lectura de datos desde KAFKA

df = spark \
    .readStream \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    ) \
    .option(
        "subscribe",
        "sensor_data"
    ) \
    .load()

# Convertir JSON a columnas SPARK

parsed_df = df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# Análisis de datos en tiempo real

# Cálculo del promedio de la TRM por Moneda

promedio_trm = parsed_df.gr*upBy(
    "tipo_moneda"
).agg(
   *avg("valor").alias("promedio_trm")*)

# Muestra de resultados en consola

query = promedio_trm \
    .writ*
