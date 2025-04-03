from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, floor, year, to_date, when, lit
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from datetime import datetime

def analyze_age_data():
    spark = SparkSession.builder \
        .appName("Análisis de Edades RENIEC") \
        .config("spark.driver.memory", "8G") \
        .getOrCreate()

    try:
        # Cargar datos
        df = spark.read.csv(
            "RENIEC10GB_CSV.csv",
            sep=";",
            header=True,
            inferSchema=True
        )

        # Verificar columnas necesarias
        if "nacimiento" not in df.columns or "edad" not in df.columns:
            messagebox.showerror("Error", "El CSV no tiene las columnas 'nacimiento' y/o 'edad'")
            return

        # ==================================================
        # VALIDACIONES Y LIMPIEZA DE DATOS
        # ==================================================
        
        # 1. Limpiar datos nulos o inválidos
        df = df.na.drop(subset=["documento"])  # Mantener solo registros con documento
        
        # 2. Validación de fechas de nacimiento
        df = df.withColumn("nacimiento_valido", 
                         when((col("nacimiento").isNotNull()) &
                              (col("nacimiento") != "0000-00-00") &
                              (to_date(col("nacimiento"), "yyyy-MM-dd").isNotNull()),
                         to_date(col("nacimiento"), "yyyy-MM-dd")).otherwise(None))
        
        # 3. Validar rango de años de nacimiento (1800-2025)
        df = df.withColumn("anio_nacimiento", year(col("nacimiento_valido")))
        df = df.withColumn("nacimiento_valido",
                          when((col("anio_nacimiento") >= 1500) & 
                               (col("anio_nacimiento") <= 2025),
                          col("nacimiento_valido")).otherwise(None))
        
        # 4. Validación de edades (0 < edad < 150)
        df = df.withColumn("edad_valida",
                          when((col("edad").isNotNull()) &
                               (col("edad") > 0) &
                               (col("edad") < 500),
                          col("edad")).otherwise(None))
        
        # 5. Calcular edad real basada en fecha de nacimiento (año 2025)
        df = df.withColumn("edad_real", 
                          when(col("nacimiento_valido").isNotNull(),
                               2025 - year(col("nacimiento_valido"))).otherwise(None))
        
        # 6. Filtrar solo registros con al menos edad válida o fecha válida
        df_validos = df.filter((col("edad_valida").isNotNull()) | 
                              (col("nacimiento_valido").isNotNull()))

        # ==================================================
        # ANÁLISIS CON DATOS VALIDADOS
        # ==================================================

        # 1. Análisis de edades extremas (solo edades válidas)
        print("\n" + "="*50)
        print(" 📊 ANÁLISIS DE EDADES EXTREMAS (VALIDADO)")
        print("="*50)

        # 3 personas más jóvenes (edad mínima válida)
        youngest = df_validos.filter(col("edad_valida").isNotNull()) \
                           .orderBy("edad_valida") \
                           .limit(3)
        print("\n👶 3 PERSONAS MÁS JÓVENES (edad mínima válida):")
        youngest.select("documento", "paterno", "materno", "nombres", "edad_valida", "nacimiento_valido").show(truncate=False)

        # 3 personas más mayores (edad máxima válida)
        oldest = df_validos.filter(col("edad_valida").isNotNull()) \
                          .orderBy(col("edad_valida").desc()) \
                          .limit(3)
        print("\n🧓 3 PERSONAS MÁS MAYORES (edad máxima válida):")
        oldest.select("documento", "paterno", "materno", "nombres", "edad_valida", "nacimiento_valido").show(truncate=False)

        # 2. Análisis de fechas de nacimiento (solo fechas válidas)
        print("\n" + "="*50)
        print(" 🎂 ANÁLISIS DE FECHAS DE NACIMIENTO (VALIDADO)")
        print("="*50)

        # 3 fechas más antiguas válidas
        oldest_birth = df_validos.filter(col("nacimiento_valido").isNotNull()) \
                                .orderBy("nacimiento_valido") \
                                .limit(3)
        print("\n🕰️ 3 FECHAS DE NACIMIENTO MÁS ANTIGUAS (válidas):")
        oldest_birth.select("documento", "paterno", "materno", "nombres", "nacimiento_valido", "edad_real").show(truncate=False)

        # 3 fechas más recientes válidas
        newest_birth = df_validos.filter(col("nacimiento_valido").isNotNull()) \
                                .orderBy(col("nacimiento_valido").desc()) \
                                .limit(3)
        print("\n👶 3 FECHAS DE NACIMIENTO MÁS RECIENTES (válidas):")
        newest_birth.select("documento", "paterno", "materno", "nombres", "nacimiento_valido", "edad_real").show(truncate=False)

        # 3. Comparación edad registrada vs edad real (solo donde ambas son válidas)
        print("\n" + "="*50)
        print(" 🔍 COMPARACIÓN EDAD REGISTRADA vs EDAD REAL (VALIDADO)")
        print("="*50)
        comparison = df_validos.filter((col("edad_valida").isNotNull()) & 
                                      (col("edad_real").isNotNull())) \
                             .select(
                                 "documento", 
                                 "nombres", 
                                 "paterno", 
                                 "edad_valida", 
                                 "edad_real",
                                 (col("edad_real") - col("edad_valida")).alias("diferencia")
                             ).orderBy(col("diferencia").abs().desc()).limit(10)
        
        comparison.show(truncate=False)

        # 4. Estadísticas de limpieza
        total_registros = df.count()
        registros_validos = df_validos.count()
        print("\n" + "="*50)
        print(" 📝 ESTADÍSTICAS DE LIMPIEZA DE DATOS")
        print("="*50)
        print(f"Total de registros: {total_registros:,}")
        print(f"Registros válidos (con edad o fecha válida): {registros_validos:,}")
        print(f"Porcentaje de datos válidos: {(registros_validos/total_registros)*100:.2f}%")

    finally:
        spark.stop()

if __name__ == "__main__":
    analyze_age_data()