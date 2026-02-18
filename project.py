import os
import time
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("conversionInicial")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    tiempo_inicio = time.time()

    # esquema para el archivo measurements.txt
    fileSchema = StructType([
        StructField("estacion", StringType(), True),
        StructField("medida", DoubleType(), True)
    ])

    # Conversion inicial a formato parquet para acelerar el analisis
    # Solo se efectuara si no existe previamente el directorio
    path="/measurements"
    if not os.path.exists(path):
        DFcrudos = spark.read.csv("measurements.txt",
                                  sep=";",
                                  header=False,
                                  schema=fileSchema)
        DFcrudos.write.mode("ignore").parquet("measurements", compression="snappy")

    # inicio del analisis con los archivos parquet
    DFmedidas = spark.read.parquet("measurements")
    DFmedidas.createOrReplaceTempView("SQLdf")
    query = '''SELECT estacion, MIN(medida) AS temp_min, 
                      MAX(medida) AS temp_max, ROUND(AVG(medida), 2) AS temp_promedio
               FROM SQLdf 
               GROUP BY estacion
               ORDER BY estacion'''
    analisis = spark.sql(query)
    # creación de un CSV con los resultados del análisis
    analisis.write.csv("/opt/spark/work-dir/analisis", mode="overwrite", header=True)

    # creación de un CSV con las estaciones mas altas y mas bajas
    analisis.createOrReplaceTempView("SQLanalisis")
    query2 = '''SELECT estacion, temp_promedio 
                FROM SQLanalisis 
                WHERE temp_promedio = (SELECT MIN(temp_promedio) FROM SQLanalisis)
                OR temp_promedio = (SELECT MAX(temp_promedio) FROM SQLanalisis)'''
    estacionesMinMax = spark.sql(query2)
    estacionesMinMax.write.csv("/opt/spark/work-dir/estacionesMinMax",
                               mode="overwrite", header=True)

    tiempo_final = time.time()

    print(f"Tiempo de ejecucion total: {tiempo_final - tiempo_inicio:.2f} segundos")
    print("revisa el DAG en http://localhost:4040")
    # pausa por 5 minutos para entrar a
    # http://localhost:4040
    # comment out para eliminar el retardo
    time.sleep(300)

    spark.stop()