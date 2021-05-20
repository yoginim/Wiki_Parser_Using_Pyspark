# Wiki_Parser_Using_Pyspark

PySpark is the Python API written in python to support Apache Spark. Apache Spark is a distributed framework that can handle Big Data analysis. Apache Spark is written in Scala and can be integrated with Python, Scala, Java, R, SQL  languages. Spark is basically a computational engine, that works with huge sets of data by processing them in parallel and batch systems.

### Some of the concepts which are used in this project:

1. Resilient Distributed Datasets (RDDs) : RDD is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel. RDDs are immutable elements, which means once you create an RDD you cannot change it. RDDs are fault tolerant as well, hence in case of any failure, they recover automatically. You can apply multiple operations on a RDD to create a new RDD.
2. PySparkSQL: A PySpark library to apply SQL-like analysis on a huge amount of structured or semi-structured data. We can also use SQL queries with PySparkSQL.  PySparkSQL is a wrapper over the PySpark core. It introduced the DataFrame, a tabular representation of structured data that is similar to that of a table from a relational database management system.
3. Graphframes: The GraphFrames is a purpose graph processing library that provides a set of APIs for performing graph analysis efficiently, using the PySpark core and PySparkSQL. It is optimized for fast distributed computing.

##### This project includes following steps:
1. Data cleaning and processing
2. Graph creation
3. Basic queries on the constructed graph
4. Message Aggregation - Page ranking 

#### Python libraries used are: pyspark, Graphframes, OS, re
