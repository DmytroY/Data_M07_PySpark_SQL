import os
import pyspark
from pyspark.sql import SparkSession, SQLContext

spark = SparkSession.builder.appName("trying_delta")\
  .config("spark.jars.packages", ",io.delta:delta-core_2.12:2.2.0")\
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
  .getOrCreate()

print("=============== Curent directory: ")
print(os.getcwd())

with open("notebooks/select.sql") as sql_file:
  query = sql_file.read()

print(" ========= SQL query:", query)

result = spark.sql(query)
result.show()

# df = (spark
#         .read
#         .format('csv')
#         .option('header', 'true')
#         .option('inferSchema', 'true')
#         .load('data/dataset.csv'))

# df.show()

# df.write.format('delta').saveAsTable('test_table')

spark.stop()