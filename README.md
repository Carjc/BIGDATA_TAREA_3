**Descripción**
Se implementa una arquitectura de procesamiento de datos en tiempo real utilizando Apache Kafka y Apache Spark Streaming.

Se utiliza un archivo CSV con información histórica de la Tasa Representativa del Mercado (TRM) de Colombia. Los datos son enviados a Kafka mediante un productor desarrollado en Python y posteriormente consumidos por Spark Streaming para realizar un análisis en tiempo real.

El objetivo es comprender cómo funcionan Kafka y Spark Streaming para la transmisión y procesamiento de datos dentro de un entorno Big Data.

**Herramientas utilizadas**.
Ubuntu 22.04
Python 3
Apache Kafka 3.9.2
Apache Spark 3.5.3
ZooKeeper
VirtualBox

**El flujo de proceso:**

Archivo CSV
     │
     ▼
Kafka Producer
     │
     ▼
Topic Kafka (sensor_data)
     │
     ▼
Spark Streaming Consumer
     │
     ▼
Promedio de la TRM

**Ejecución**

1. Iniciar ZooKeeper
2. Iniciar Kafka
3. Crear el sensor
4. Ejecutar el productor
5. Ejecutar el consumidor
6. 
