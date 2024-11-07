from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CensoEscolar_UF_Dependencia").getOrCreate()

# Pré-processamento
df = spark.read.csv("censo_escolar_2021.csv", sep=";", header=True)
df_cleaned = df.dropna(subset=["SG_UF", "TP_DEPENDENCIA"])

# Agrupar por UF e dependência administrativa, ordenando por contagem
escolas_por_uf = df_cleaned.groupBy("SG_UF", "TP_DEPENDENCIA").count().orderBy("count", ascending=False)

# Salvar o resultado em um arquivo CSV
escolas_por_uf.write.csv("escolas_por_uf_dependencia.csv", header=True)