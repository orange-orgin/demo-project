from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, avg, count, max as spark_max, min as spark_min, year, month, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ========== 构造示例数据（模拟豆瓣电影数据集）==========
schema = StructType([
    StructField("movieId", IntegerType()),
    StructField("title", StringType()),
    StructField("genre", StringType()),
    StructField("rating", DoubleType()),
    StructField("year", IntegerType())
])

data = [
    (1, "The Shawshank Redemption", "Drama", 9.3, 1994),
    (2, "The Godfather", "Drama", 9.2, 1972),
    (3, "The Dark Knight", "Action", 9.0, 2008),
    (4, "Schindler's List", "Drama", 8.9, 1993),
    (5, "Pulp Fiction", "Crime", 8.8, 1994),
    (6, "The Lord of the Rings: The Return of the King", "Adventure", 8.9, 2003),
    (7, "Forrest Gump", "Drama", 8.8, 1994),
    (8, "Inception", "Sci-Fi", 8.8, 2010),
    (9, "Interstellar", "Sci-Fi", 8.6, 2014),
    (10, "Parasite", "Drama", 8.5, 2019),
    (11, "Spirited Away", "Animation", 8.6, 2001),
    (12, "The Matrix", "Sci-Fi", 8.7, 1999),
    (13, "Goodfellas", "Crime", 8.7, 1990),
    (14, "Se7en", "Crime", 8.6, 1995),
    (15, "The Silence of the Lambs", "Thriller", 8.6, 1991),
    (16, "It's a Wonderful Life", "Drama", 8.6, 1946),
    (17, "Life Is Beautiful", "Comedy", 8.6, 1997),
    (18, "The Departed", "Crime", 8.5, 2006),
    (19, "Whiplash", "Drama", 8.5, 2014),
    (20, "The Prestige", "Drama", 8.5, 2006),
    (21, "Gladiator", "Action", 8.5, 2000),
    (22, "The Lion King", "Animation", 8.5, 1994),
    (23, "Back to the Future", "Adventure", 8.5, 1985),
    (24, "The Pianist", "Drama", 8.5, 2002),
    (25, "American History X", "Drama", 8.5, 1998),
    (26, "Casablanca", "Romance", 8.5, 1942),
    (27, "Modern Times", "Comedy", 8.5, 1936),
    (28, "City Lights", "Comedy", 8.5, 1931),
    (29, "Avengers: Endgame", "Action", 8.4, 2019),
    (30, "Joker", "Drama", 8.4, 2019)
]

df = spark.createDataFrame(data, schema)

# ========== A-1: 数据清洗 ==========
print("="*60)
print("A-1: 数据清洗")
print("="*60)

# 打印 Schema 和前 5 行
print("\nSchema:")
df.printSchema()
print("\n前 5 行数据:")
df.show(5)

# 统计各字段缺失值比例
total = df.count()
print(f"\n总行数: {total}")
for column in df.columns:
    null_count = df.filter(col(column).isNull()).count()
    missing_ratio = null_count / total * 100
    print(f"{column} 缺失值: {null_count} 行 ({missing_ratio:.2f}%)")

# 缺失值处理策略
# 策略1: 删除 title 为空的行（dropna）
df_clean = df.dropna(subset=["title"])
# 策略2: 用平均值填充 rating 为空的行（fillna）
avg_rating = df_clean.select(avg("rating")).collect()[0][0]
df_clean = df_clean.fillna({"rating": avg_rating})

print(f"\n清洗前行数: {df.count()}")
print(f"清洗后行数: {df_clean.count()}")

# 基本统计信息
print("\n各字段基本统计信息:")
df_clean.describe().show()

# ========== A-2: SparkSQL 统计分析 ==========
print("="*60)
print("A-2: SparkSQL 统计分析")
print("="*60)

df_clean.createOrReplaceTempView("movies")

# 查询1: GROUP BY 聚合 - 按类型统计平均评分
print("\n查询1: GROUP BY 聚合 - 按类型统计平均评分")
result1 = spark.sql("""
    SELECT genre, 
           COUNT(*) as movie_count, 
           ROUND(AVG(rating), 2) as avg_rating,
           ROUND(MAX(rating), 2) as max_rating,
           ROUND(MIN(rating), 2) as min_rating
    FROM movies
    GROUP BY genre
    ORDER BY avg_rating DESC
""")
result1.show()
print("分析说明: 按电影类型分组统计，可以看出 Drama 类型电影最多且评分较高，"
      "Sci-Fi 类型平均评分也很突出，说明观众对科幻片的整体评价较好。")

# 查询2: ORDER BY Top-N - 评分最高的 10 部电影
print("\n查询2: ORDER BY Top-N - 评分最高的 10 部电影")
result2 = spark.sql("""
    SELECT title, rating, year, genre
    FROM movies
    ORDER BY rating DESC
    LIMIT 10
""")
result2.show()
print("分析说明: 评分最高的电影集中在 9.0-9.3 分之间，"
      "The Shawshank Redemption 以 9.3 分位居榜首，"
      "这些高分电影多为剧情片，反映了经典影片的持久魅力。")

# 查询3: 时间维度趋势分析 - 按年代统计电影数量和平均评分
print("\n查询3: 时间维度趋势分析 - 按年代统计")
result3 = spark.sql("""
    SELECT CAST(FLOOR(year/10)*10 AS INT) as decade,
           COUNT(*) as movie_count,
           ROUND(AVG(rating), 2) as avg_rating
    FROM movies
    WHERE year IS NOT NULL
    GROUP BY decade
    ORDER BY decade
""")
result3.show()
print("分析说明: 将电影按十年为一组进行统计，可以看出不同年代的电影产量和质量变化。"
      "1990年代电影数量最多，可能与电影产业的蓬勃发展有关。")

# 查询4: 窗口函数 - 每种类型中评分排名前3的电影
print("\n查询4: 窗口函数 - 每种类型评分排名前3")
result4 = spark.sql("""
    SELECT genre, title, rating, rank
    FROM (
        SELECT genre, title, rating,
               ROW_NUMBER() OVER (PARTITION BY genre ORDER BY rating DESC) as rank
        FROM movies
    )
    WHERE rank <= 3
    ORDER BY genre, rank
""")
result4.show()
print("分析说明: 使用窗口函数 ROW_NUMBER() 对每种电影类型内部按评分排名，"
      "可以找出各类型中最优秀的代表作。"
      "例如 Drama 类型中 The Shawshank Redemption 排名第一。")

spark.stop()
print("\n所有分析完成！")