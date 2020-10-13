from pyspark.sql import SparkSession

import time
import os


nthreads = 8
q = "SELECT sum(l_extendedprice), sum(l_tax), sum(l_discount) FROM lineitem"
# q = "SELECT sum(l_extendedprice), sum(l_tax), sum(l_discount) FROM lineitem JOIN orders ON l_orderkey=o_orderkey WHERE l_quantity > 49"

repeats = 3

sf = 100
outdir = '/export/scratch2/home/hannes/cost/'
parquet_lineitem_folder = os.path.join(outdir, "sf%d.parquet" % sf)
parquet_orders_folder = os.path.join(outdir, "sf%d-orders.parquet" % sf)

memory_gb = 10

spark = SparkSession.builder.master("local[%d]" % nthreads).config("spark.ui.enabled", "false").config("spark.local.dir", outdir).config("spark.driver.memory", "%dg" % memory_gb).config("spark.executor.memory", "%dg" % memory_gb).getOrCreate()
sc = spark.sparkContext


parquet_lineitem = spark.read.parquet(parquet_lineitem_folder)
parquet_lineitem.createOrReplaceTempView("lineitem")

#parquet_orders = spark.read.parquet(parquet_orders_folder)
#parquet_orders.createOrReplaceTempView("orders")

for i in range(repeats):
	start_time = time.monotonic()
	result = spark.sql(q)
	result.take(1)
	this_time = time.monotonic() - start_time
	print("spark\t%d\t%d\t%f" % (sf, i, this_time))


import duckdb
import time
import os


con = duckdb.connect()
con.execute("PRAGMA threads=%d" % nthreads)
con.execute("CREATE VIEW lineitem AS SELECT * FROM parquet_scan('sf%s.parquet/part*')" % sf)

for i in range(repeats):
	start_time = time.monotonic()
	result = con.execute(q)
	result.fetchone()
	this_time = time.monotonic() - start_time
	print("duckdb\t%d\t%d\t%f" % (sf, i, this_time))
