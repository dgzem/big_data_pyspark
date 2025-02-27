# Big Data Tools - PySpark

Hello developers, welcome to my repository for Pyspark studies recap 😄

In here you will find the following files:

* Dockerfile: Custom Python Image that is being created to run Pyspark;
* docker-compose.yaml: List of containers that it is necessary ro run Pyspark, such as:

  * Spark-master node;
  * Spark-worker node;
  * Pyspark image (being custom build with the help of Dockerfile).
* data: In this directory it is being put the data that is going to be loaded into Pyspark so manipulations can be done, suche as:

  * Data Frames Manipulations;
  * Data Frames Optimizations;
  * Data Sets;
  * RDDs;


## Data Frames Manipulations

Main File: [df_manipulation.py](https://github.com/dgzem/big_data_pyspark/blob/main/df_manipulation.py)

### What is being done?

Features & SQL Equivalents

* Selection & Filtering (SELECT, WHERE, DISTINCT) ✔️
* Aggregation & Grouping (GROUP BY, HAVING, AVG, MAX, MIN) ✔️
* Conditional Transformations (CASE WHEN) ✔️
* Sorting (ORDER BY) ✔️
* Joins (INNER JOIN, LEFT JOIN, CROSS JOIN) ✔️
* Window Functions (ROW_NUMBER, RANK, DENSE_RANK) 🚧
* String & Numeric Operations (COALESCE, LOWER, COUNT, SPLIT) ✔️
