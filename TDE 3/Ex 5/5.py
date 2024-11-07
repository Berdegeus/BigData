from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CensoEscolar_Localizacao_Dependencia").getOrCreate()

# Pré-processamento
df = spark.read.csv("censo_escolar_2021.csv", sep=";", header=True)
df_cleaned = df.dropna(subset=["TP_LOCALIZACAO", "TP_DEPENDENCIA"])

# Agrupar por tipo de localização e dependência administrativa
escolas_por_tipo = df_cleaned.groupBy("TP_LOCALIZACAO", "TP_DEPENDENCIA").count()

# Salvar o resultado em um arquivo CSV
escolas_por_tipo.write.csv("escolas_por_localizacao_dependencia.csv", header=True)