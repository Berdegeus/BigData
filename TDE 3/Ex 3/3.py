from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("CensoEscolar_Docentes").getOrCreate()

# Pré-processamento
df = spark.read.csv("censo_escolar_2021.csv", sep=";", header=True)
df_cleaned = df.dropna(subset=["QT_DOC_BAS", "QT_DOC_FUND", "QT_DOC_MED"])

# Calcular total de docentes por escola
df_cleaned = df_cleaned.withColumn("TOTAL_DOCENTES", 
                                   col("QT_DOC_BAS").cast("int") + col("QT_DOC_FUND").cast("int") + col("QT_DOC_MED").cast("int"))

# Identificar a escola com mais docentes
escola_maior_docentes = df_cleaned.orderBy(col("TOTAL_DOCENTES").desc()).select("NO_ENTIDADE", "NO_MUNICIPIO", "TOTAL_DOCENTES").first()

# Salvar o resultado em um arquivo de texto
with open("maior_num_docentes.txt", "w") as f:
    f.write(f"Escola com o maior número de docentes: {escola_maior_docentes}")