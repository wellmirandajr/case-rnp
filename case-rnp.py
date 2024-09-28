# Databricks notebook source
# MAGIC %md
# MAGIC ###Libs

# COMMAND ----------

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ###Configurações de conexão

# COMMAND ----------

host = "psql-mock-database-cloud.postgres.database.azure.com"
database = "ecom1692155331663giqokzaqmuqlogbu"
port = "5432"
username = "eolowynayhvayxbhluzaqxfp@psql-mock-database-cloud"
password = "hdzvzutlssuozdonhflhwyjm"

# URL
url = f"jdbc:postgresql://{host}:{port}/{database}"

# COMMAND ----------

# Inicializando a sessão Spark
spark = SparkSession.builder \
    .appName("PostgreSQL Connection") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Acessando ao Banco

# COMMAND ----------

tables_df = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') as tables") \
    .option("user", username) \
    .option("password", password) \
    .load()

# Exibindo as tabelas
tables_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Realizando discovery da tabela Customers

# COMMAND ----------

# Lendo dados do banco de dados
customers_jdbc = spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "customers") \
    .option("user", username) \
    .option("password", password) \
    .load()

# COMMAND ----------

# Exibindo os dados
display(customers_jdbc)

# COMMAND ----------

columns_info = customers_jdbc.schema

# COMMAND ----------

# Exibindo informações das colunas
for field in columns_info.fields:
    print(f"Nome da Coluna: {field.name}, Tipo: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando as tabelas no formato de Parquet

# COMMAND ----------

table_names = ["customers", "employees", "orderdetails", "orders", "payments", "product_lines", "products", "offices"]

# Diretório onde os arquivos Parquet serão salvos
output_directory = "/dbfs/FileStore/tables/case_rnp"

# Loop para realizar a ingestão dos arquivos com o formart Parquet
for table_name in table_names:
    # Lendo os dados da tabela
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .load()
    parquet_path = f"{output_directory}/{table_name}.parquet"
    df.write.parquet(parquet_path)


# COMMAND ----------

# Loop para realizar a leitura dos aquivos Parquet
arquivos = dbutils.fs.ls(output_directory)
parquet_files = [arquivo.path for arquivo in arquivos if arquivo.path.endswith(".parquet")]
for arquivo in parquet_files:
    print(arquivo)
    table_name = parquet_file.split("/")[-1].replace(".parquet", "")
    df = spark.read.parquet(parquet_file)

# COMMAND ----------

# Path dos arquivos salvos no DBFS
display(arquivos)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Criando os DataFrames

# COMMAND ----------

customers = spark.read.parquet("dbfs:/dbfs/FileStore/tables/RNP/TABLES_RNP/customers.parquet/")
employees = spark.read.parquet("dbfs:/dbfs/FileStore/tables/RNP/TABLES_RNP/employees.parquet/")
offices = spark.read.parquet("dbfs:/dbfs/FileStore/tables/case_rnp/offices.parquet/")
orderdetails = spark.read.parquet("dbfs:/dbfs/FileStore/tables/RNP/TABLES_RNP/orderdetails.parquet/")
orders = spark.read.parquet("dbfs:/dbfs/FileStore/tables/RNP/TABLES_RNP/orders.parquet/")
payments = spark.read.parquet("dbfs:/dbfs/FileStore/tables/RNP/TABLES_RNP/payments.parquet/")
product_lines = spark.read.parquet("dbfs:/dbfs/FileStore/tables/RNP/TABLES_RNP/product_lines.parquet/")
products = spark.read.parquet("dbfs:/dbfs/FileStore/tables/RNP/TABLES_RNP/products.parquet/")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Criando as tabelas no Schema RNP

# COMMAND ----------

# Lista de DataFrames
dataframes = {
    "customers": customers,
    "employees": employees,
    "offices": offices,
    "orderdetails": orderdetails,
    "orders": orders,
    "payments": payments,
    "product_lines": product_lines,
    "products": products
}

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", 'America/Sao_Paulo')

# COMMAND ----------

# Criando Schema no Catalogo
spark.sql("CREATE SCHEMA IF NOT EXISTS rnp")
spark.sql("USE SCHEMA rnp")

# Loop para salvar cada DataFrame como tabela com a coluna de ingestão
for table_name, df in dataframes.items():
    # Adicionando coluna de ts_ingestao_brt
    df_with_timestamp = df.withColumn(
        "ts_ingestao_brt", 
        F.from_utc_timestamp(F.current_timestamp(), 'America/Sao_Paulo')
    ).withColumn("dt_ingestao_brt", F.current_date())
    
    # Adicionando o prefixo 'tb_'
    full_table_name = f"tb_{table_name}"
    
    # Definindo o caminho onde a tabela Delta será salva
    delta_table_path = f"/delta/rnp/{full_table_name}"
    
    # Salvando a tabela em formato Delta
    df_with_timestamp.write.format("delta").mode("overwrite").save(delta_table_path)

    # Tabela criada com sucesso
    print(f"Tabela Delta {full_table_name} criada com sucesso.")


# COMMAND ----------

# MAGIC %md
# MAGIC #4. Criar as querys ou código utilizando a linguagem de sua preferência que respondam as seguintes perguntas:

# COMMAND ----------

# MAGIC %md
# MAGIC ###Qual país possui a maior quantidade de itens cancelados?

# COMMAND ----------

# Inicializa a Spark session
spark = SparkSession.builder.appName("DeltaTableExample").getOrCreate()

# Tabelas
order_details_df = spark.table("rnp.tb_orderdetails")
orders_df = spark.table("rnp.tb_orders")
customers_df = spark.table("rnp.tb_customers")

# COMMAND ----------

# Join Tabelas
tb_canceled_items_per_country = (
    order_details_df
    .join(orders_df, order_details_df.order_number == orders_df.order_number)
    .join(customers_df, orders_df.customer_number == customers_df.customer_number)
    .where(orders_df.status == 'Cancelled')
    .groupBy(customers_df.country)
    .agg(F.count(order_details_df.quantity_ordered).alias("canceled_items_count"))
    .orderBy(F.desc("canceled_items_count"))
)

display(vw_canceled_items_per_country)

# COMMAND ----------

# Definindo o caminho da tabela Delta no schema rnp
delta_table_path = "/delta/rnp/tb_canceled_items_per_country"

# Adicionando as colunas de ingestão
tb_canceled_items_per_country_with_timestamp = tb_canceled_items_per_country.withColumn(
    "ts_ingestao_brt", 
    F.from_utc_timestamp(F.current_timestamp(), 'America/Sao_Paulo')
).withColumn("dt_ingestao_brt", F.current_date())

# Salvando o resultado como uma tabela Delta no esquema rnp
tb_canceled_items_per_country_with_timestamp.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(delta_table_path)

# Registrando a tabela no catálogo do Spark
spark.sql(f"CREATE TABLE IF NOT EXISTS rnp.tb_canceled_items_per_country USING DELTA LOCATION '{delta_table_path}'")

print("Tabela Delta 'tb_canceled_items_per_country' criada com sucesso.")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Resultado utilizando SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from rnp.tb_canceled_items_per_country limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Qual o faturamento da linha de produto mais vendido, considere os pedidos com status 'Shipped', cujo o pedido foi realizado no ano de 2005?

# COMMAND ----------

# Tabelas
order_details_df = spark.table("rnp.tb_orderdetails")
orders_df = spark.table("rnp.tb_orders")
products_df = spark.table("rnp.tb_products")
product_lines_df = spark.table("rnp.tb_product_lines")

# COMMAND ----------

# Join Tabelas
tb_total_revenue_per_product_line = (
    order_details_df
    .join(orders_df, order_details_df.order_number == orders_df.order_number)
    .join(products_df, order_details_df.product_code == products_df.product_code)
    .join(product_lines_df, products_df.product_line == product_lines_df.product_line)
    .where((orders_df.status == 'Shipped') & (F.year(orders_df.order_date) == 2005))
    .groupBy(product_lines_df.product_line)
    .agg(F.round(F.sum(order_details_df.quantity_ordered * order_details_df.price_each), 2).alias("total_revenue"))
    .orderBy(F.desc("total_revenue"))
)

display(tb_total_revenue_per_product_line)

# COMMAND ----------

# Definindo o caminho da tabela Delta no schema rnp
delta_table_path = "/delta/rnp/tb_total_revenue_per_product_line"

# Adicionando as colunas de ingestão
tb_total_revenue_per_product_line_with_timestamp = tb_total_revenue_per_product_line.withColumn(
    "ts_ingestao_brt", 
    F.from_utc_timestamp(F.current_timestamp(), 'America/Sao_Paulo')
).withColumn("dt_ingestao_brt", F.current_date())

# Salvando o resultado como uma tabela Delta no esquema rnp
tb_total_revenue_per_product_line_with_timestamp.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(delta_table_path)

# Registrando a tabela no catálogo do Spark
spark.sql(f"CREATE TABLE IF NOT EXISTS rnp.tb_total_revenue_per_product_line USING DELTA LOCATION '{delta_table_path}'")

print("Tabela Delta 'tb_total_revenue_per_product_line' criada com sucesso.")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Resultado utilizando SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_line,total_revenue from rnp.tb_total_revenue_per_product_line limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC ###Traga na consulta o Nome, sobrenome e e-mail dos vendedores do Japão, lembrando que o local-part do e-mail deve estar mascarado.

# COMMAND ----------

# Tabelas
employees_df = spark.table("rnp.tb_employees")
offices_df = spark.table("rnp.tb_offices")

# COMMAND ----------

# Join Tabelas
tb_employees_japan = (
    employees_df
    .join(offices_df, employees_df.office_code == offices_df.office_code, "left")
    .where(offices_df.country == "Japan")
    .select(
        F.concat(employees_df.first_name, F.lit(" "), employees_df.last_name).alias("employee_name"),
        # Usando F.expr para mascarar o e-mail
        F.expr("concat(substring(email, 1, 2), '****', substring(email, instr(email, '@'), length(email)))").alias("email")
    )
)

display(tb_employees_japan)

# COMMAND ----------

# Definindo o caminho da tabela Delta no schema rnp
delta_table_path = "/delta/rnp/tb_employees_japan"

# Adicionando as colunas de ingestão
tb_employees_japan_with_timestamp = tb_employees_japan.withColumn(
    "ts_ingestao_brt", 
    F.from_utc_timestamp(F.current_timestamp(), 'America/Sao_Paulo')
).withColumn("dt_ingestao_brt", F.current_date())

# Salvando o resultado como uma tabela Delta no esquema rnp
tb_employees_japan_with_timestamp.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(delta_table_path)

# Registrando a tabela no catálogo do Spark
spark.sql(f"CREATE TABLE IF NOT EXISTS rnp.tb_employees_japan USING DELTA LOCATION '{delta_table_path}'")

print("Tabela Delta 'tb_employees_japan' criada com sucesso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Resultado utilizando SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from rnp.tb_employees_japan
