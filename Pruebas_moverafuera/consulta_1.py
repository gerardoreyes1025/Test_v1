from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, lpad

def init_spark():
    return SparkSession.builder \
        .appName("Análisis Estructura RENIEC") \
        .config("spark.driver.memory", "8G") \
        .getOrCreate()

def analyze_structure():
    spark = init_spark()
    
    try:
        # Cargar datos
        df = spark.read.csv(
            "RENIEC10GB_CSV.csv",
            sep=";",
            header=True,
            inferSchema=True
        )
        
        # 1. Contar total de filas
        total_filas = df.count()
        print(f"\n📊 Total de filas en el dataset: {total_filas:,}")
        
        # 2. Analizar campo 'documento'
        # - Asegurar que todos tengan 8 dígitos (rellenar con ceros a la izquierda)
        df = df.withColumn("documento_completo", 
                         lpad(col("documento").cast("string"), 8, "0"))
        
        # - Encontrar los 3 documentos más pequeños
        print("\n🔽 3 Documentos MÁS PEQUEÑOS:")
        min_docs = df.orderBy("documento_completo").limit(3)
        min_docs.select("documento_completo").show(3, truncate=False)
        
        # - Encontrar los 3 documentos más grandes
        print("\n🔼 3 Documentos MÁS GRANDES:")
        max_docs = df.orderBy(col("documento_completo").desc()).limit(3)
        max_docs.select("documento_completo").show(3, truncate=False)
        
        # - Estadísticas adicionales
        print("\n🔍 Resumen estadístico del campo 'documento':")
        df.select(length("documento_completo").alias("longitud")).groupBy("longitud").count().show()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    analyze_structure()