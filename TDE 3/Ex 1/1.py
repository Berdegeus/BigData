from pyspark.sql import SparkSession

# Criação da sessão Spark
spark = SparkSession.builder.appName("CensoEscolar_Curitiba").getOrCreate()

# Pré-processamento
df = spark.read.csv("censo_escolar_2021.csv", sep=";", header=True)
df_cleaned = df.dropna(subset=["NO_MUNICIPIO"])

# Filtrar escolas em Curitiba e contar
num_escolas_curitiba = df_cleaned.filter(df_cleaned["NO_MUNICIPIO"] == "Curitiba").count()

# Salvar o resultado em um arquivo de texto
with open("num_escolas_curitiba.txt", "w") as f:
    f.write(f"Número de escolas em Curitiba: {num_escolas_curitiba}")