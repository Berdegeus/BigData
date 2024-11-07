from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("CensoEscolar_Matriculas").getOrCreate()

# Pré-processamento
df = spark.read.csv("censo_escolar_2021.csv", sep=";", header=True)
df_cleaned = df.dropna(subset=["NO_REGIAO", "QT_MAT_MED"])

# Calcular média de matrículas de Ensino Médio por região
media_matriculas = df_cleaned.groupBy("NO_REGIAO").agg(avg("QT_MAT_MED").alias("MEDIA_MATRICULAS"))

# Salvar o resultado em um arquivo CSV
media_matriculas.write.csv("media_matriculas_por_regiao.csv", header=True)