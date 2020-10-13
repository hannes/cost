import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *

parquet_compression = 'snappy'

outdir = '/export/scratch2/home/hannes/cost/'
parquet_folder = os.path.join(outdir, "sf100.parquet")
nthreads = 16
memory_gb = 10

spark = SparkSession.builder.master("local[%d]" % nthreads).config("spark.ui.enabled", "false").config("spark.local.dir", outdir).config("spark.driver.memory", "%dg" % memory_gb).config("spark.executor.memory", "%dg" % memory_gb).getOrCreate()
sc = spark.sparkContext


schema = StructType([
    StructField("l_orderkey",      LongType(),    False),
    StructField("l_partkey",       LongType(),    False),
    StructField("l_suppkey",       LongType(),    False),

    StructField("l_linenumber",    IntegerType(), False),
    StructField("l_quantity",      IntegerType(), False),

    StructField("l_extendedprice", DoubleType(),  False),
    StructField("l_discount",      DoubleType(),  False),
    StructField("l_tax",           DoubleType(),  False),
 
    StructField("l_returnflag",    StringType(),  False),
    StructField("l_linestatus",    StringType(),  False),

    StructField("l_shipdate",      StringType(),  False),
    StructField("l_commitdate",    StringType(),  False),
    StructField("l_receiptdate",   StringType(),  False),

    StructField("l_shipinstruct",  StringType(),  False),
    StructField("l_shipmode",      StringType(),  False),
    StructField("l_comment",       StringType(),  False)])

df = spark.read.format("csv").schema(schema).option("header", "false").option("delimiter", "|").load("sf100/*.tbl*")
df.write.mode('overwrite').format("parquet").save(parquet_folder)

