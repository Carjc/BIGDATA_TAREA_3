# Librerías utilizadas
import csv
import json
import time

# Cliente productor de Kafka
from kafka import KafkaProducer


# CONFIGURACIÓN

# Conexión al broker Kafka que está ejecutándose en localhost:9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],

    # Convierte los diccionarios Python a formato JSON antes de enviarlos a Kafka
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


# ARCHIVO CSV PROCESADO

csv_file = "Tasa_de_Cambio_Representativa_del__Mercado_-Historico_20260913.csv"


# LECTURA CSV

# utf-8-sig elimina caracteres especiales del encabezado
with open(csv_file, encoding='utf-8-sig') as archivo:

    # Delimitador ;
    lector = csv.DictReader(
        archivo,
        delimiter=';'
    )

    # Recorremos registro por registro
    for fila in lector:

        # Limpiamos de datos
        valor_limpio = (
            fila["VALOR"]
            .replace("$", "")
            .replace(".", "")
            .replace(",", ".")
            .strip()
        )

        # Crea los objetos que será enviado a Kafka
        sensor_data = {

            "valor": float(valor_limpio),
            "unidad": fila["UNIDAD"],
            "tipo_moneda": fila["TIPO_MONEDA"],
            "vigencia_desde": fila["VIGENCIADESDE"],
            "vigencia_hasta": fila["VIGENCIAHASTA"]
        }

        # Envío del registro al topic Kafka
        producer.send(
            "sensor_data",
            value=sensor_data
        )

        # Muestra en pantalla
        print("Enviado:", sensor_data)

        # Tiempo de espera 1 segundo entre mensajes
        time.sleep(1)


# Garantiza el envío de todos los mensajes pendientes
producer.flush()
