from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CensoEscolar_Regiao").getOrCreate()

# Pré-processamento
df = spark.read.csv("censo_escolar_2021.csv", sep=";", header=True)
df_cleaned = df.dropna(subset=["NO_REGIAO"])

# Agrupar por região e contar
escolas_por_regiao = df_cleaned.groupBy("NO_REGIAO").count().orderBy("NO_REGIAO")

# Salvar o resultado em um arquivo CSV
escolas_por_regiao.write.csv("num_escolas_por_regiao.csv", header=True)