import time, os

os.environ.setdefault("PYTHONUTF8", "1")

# ---------- 1) 用 PySpark 生成同一份随机数据，再转 pandas ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

spark = (
    SparkSession.builder
    .appName("PerfCompare-Setup")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

N = 1_000_000
print(">>> 生成随机数据 (PySpark range + rand()) ...")

df = spark.range(0, N).withColumn("val", rand())

# 关键：toPandas() 把同一份分布式数据拉回 Driver，变成真正的 pd.DataFrame
t0 = time.time()
pdf = df.select("val").toPandas()
# 现在 pdf["val"] 是真实的 ~Uniform(0,1) 采样，不是 [0]*N
mean_pd = float(pdf["val"].mean())
# Pandas 端计算耗时只计 mean（数据拉取耗时归入准备阶段更公平）
t_pd_compute = time.time() - t0

print(f"\n=== Pandas (单机, 同数据集) ===")
print(f"Pandas mean  = {mean_pd:.5f}")
print(f"Pandas compute time = {t_pd_compute:.4f}s  (toPandas拉取+mean)")

spark.stop()

# ---------- 2) PySpark 分布式计算（K8s executor 在这里干活）----------
from pyspark.sql import SparkSession as SS
from pyspark.sql.functions import rand as frand

spark2 = (
    SS.builder
    .appName("PerfCompare")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark2.sparkContext.setLogLevel("WARN")

t1 = time.time()
avg_sp = (
    spark2.range(0, N)
    .withColumn("val", frand())
    .selectExpr("avg(val)")
    .collect()[0][0]
)
t_sp = time.time() - t1
print(f"\n=== PySpark (分布式, executor 由 SparkApplication 控制) ===")
print(f"PySpark avg   = {avg_sp:.5f}")
print(f"PySpark time   = {t_sp:.4f}s")

spark2.stop()

print("\n--- A-3 实验说明 ---")
print("记录本次 executor.instances=X 的总耗时 t_sp")
print("再改 instances=1 / instances=2 分别提交两次，得 t1,t2")
print("加速比 S = t1 / t2  (预期 1 < S < 2，符合 Amdahl)")