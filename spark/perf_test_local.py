import os, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

# 如果是 JDK17+，让 JVM 允许 Spark 内部反射（3.5 更安全，但仍建议加上）
os.environ.setdefault(
    "SPARK_SUBMIT_OPTS",
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

spark = (
    SparkSession.builder
    .appName("PerfCompare")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

N = 1_000_000
print("=== PySpark 3.5 local[*] ===")
t0 = time.time()
avg = (
    spark.range(0, N)
        .withColumn("val", rand())
        .selectExpr("avg(val)")
        .collect()[0][0]
)
dt = time.time() - t0
print(f"avg(val)={avg:.5f}  t={dt:.3f}s")
spark.stop()
print("✓ 本地跑通（PySpark 3.5 / JDK21）")