-- spark-sql -f <your-script>.sql

-- SELECT "======  Hello SQL ======"
-- SELECT * FROM csv.`data/dataset.csv`;

CREATE TABLE IF NOT EXISTS users_csv (id INTEGER, name STRING, age INTEGER)
USING CSV
OPTIONS (header = "true", delimiter = ",")
LOCATION "/home/dmytro/EPAM/training/spark/m07_sparksql_python_azure/data/dataset.csv"
