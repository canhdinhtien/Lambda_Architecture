import os
import pandas as pd
import numpy as np

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col

from pyspark.ml.regression import LinearRegression
from pyspark.mllib.evaluation import RegressionMetrics

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, CrossValidatorModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator

import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.sql.functions import isnan, when, count
from pyspark.sql.functions import to_date, year

from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

pd.set_option('display.max_columns', 200)
pd.set_option('display.max_colwidth', 400)

from matplotlib import rcParams
sns.set(context='notebook', style='whitegrid', rc={'figure.figsize': (18,4)})
rcParams['figure.figsize'] = 18,4

rnd_seed=23
np.random.seed=rnd_seed
np.random.set_state=rnd_seed

spark = SparkSession.builder.master("local[2]").appName("Big-Data-Project").getOrCreate()

spark

sc = spark.sparkContext
sc

sqlContext = SQLContext(spark.sparkContext)
sqlContext

FINANCE_DATA = 'hdfs://namenode:9000/hdfs_data/finance_data.parquet'

schema = StructType([
    StructField("Ngay", StringType(), True),
    StructField("GiaDieuChinh", FloatType(), True),
    StructField("GiaDongCua", FloatType(), True),
    StructField("ThayDoi", StringType(), True),
    StructField("KhoiLuongKhopLenh", IntegerType(), True),
    StructField("GiaTriKhopLenh", FloatType(), True),
    StructField("KLThoaThuan", IntegerType(), True),
    StructField("GtThoaThuan", IntegerType(), True),
    StructField("GiaMoCua", FloatType(), True),
    StructField("GiaCaoNhat", FloatType(), True),
    StructField("GiaThapNhat", FloatType(), True),
    StructField("Symbol", StringType(), True),
])

finance_df = spark.read.parquet(FINANCE_DATA)

finance_df = finance_df.rdd.zipWithIndex() \
    .filter(lambda x: x[1] > 0) \
    .map(lambda x: x[0]) \
    .toDF()

finance_df.show(3)

finance_df.columns

finance_df.count()

finance_df.printSchema()

finance_df.select([
    count(when(col(c).isNull() | isnan(c), c)).alias(c)
    for c in finance_df.columns
]).show()

finance_df.filter(col("GtThoaThuan").isNull()).show()

finance_df_cleaned = finance_df.na.drop()

finance_df_cleaned.show(3)

from pyspark.sql.functions import sum

finance_df_cleaned.select([sum(col(c).isNull().cast("int")).alias(c) for c in finance_df_cleaned.columns]).show()

finance_df_cleaned.describe([
    "GiaDieuChinh", "GiaDongCua", "GiaTriKhopLenh",
    "GiaMoCua", "GiaCaoNhat", "GiaThapNhat"
]).show()

bank_symbols = ["ABB", "ACB", "BAB", "BID", "CTG", "EIB", "HDB",
                "KLB", "LPB", "SHB", "TCB", "STB", "TPB", "VCB", "VIB"]

finance_df_banks = finance_df_cleaned.filter(col("Symbol").isin(bank_symbols))
finance_df_banks.select("Symbol").distinct().show()
finance_df_banks.show(3)


finance_df_banks = finance_df_banks.withColumn(
    "percent_change",
    ((col("GiaDongCua") - col("GiaMoCua")) / col("GiaMoCua")) * 100
)

finance_df_banks = finance_df_banks.withColumn("Ngay_date", to_date(col("Ngay"), "dd/MM/yyyy"))
finance_df_banks = finance_df_banks.withColumn("Year", year(col("Ngay_date")))

plot_df = finance_df_banks.select("Ngay_date", "percent_change", "Symbol").toPandas()
unique_symbols = plot_df["Symbol"].unique()

for symbol in unique_symbols:
    bank_df = plot_df[plot_df["Symbol"] == symbol]

    plt.figure(figsize=(18, 4))
    sns.lineplot(data=bank_df, x="Ngay_date", y="percent_change")
    plt.title(f"Phần trăm thay đổi giá theo ngày của ngân hàng: {symbol}")
    plt.xlabel("Năm")
    plt.ylabel("Thay đổi (%)")

    plt.gca().xaxis.set_major_locator(plt.matplotlib.dates.YearLocator())
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y'))

    plt.tight_layout()
    plt.show()

cols_to_describe = [
    "GiaDieuChinh", "GiaDongCua", "KhoiLuongKhopLenh", "GiaTriKhopLenh",
    "KLThoaThuan", "GtThoaThuan", "GiaMoCua", "GiaCaoNhat", "GiaThapNhat", "percent_change"
]

describe_df = finance_df_banks.select(["Symbol"] + cols_to_describe).toPandas()
symbols = describe_df["Symbol"].unique()

for symbol in symbols:
    print(f"\nThống kê mô tả cho ngân hàng: {symbol}\n")
    bank_df = describe_df[describe_df["Symbol"] == symbol]
    display(bank_df.describe().T)

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import regexp_extract
from pyspark.sql import functions as F

finance_df_banks = finance_df_banks.withColumn(
    "ThayDoi_value",
    regexp_extract("ThayDoi", r"([-+]?[0-9]*\.?[0-9]+)", 1).cast("double")
)

symbols = [row["Symbol"] for row in finance_df_banks.select("Symbol").distinct().collect()]

results = []

for sym in symbols:
    print(f"\n----- Ngân hàng: {sym} -----")

    df_sym = finance_df_banks.filter(F.col("Symbol") == sym)

    # Tạo features vector
    assembler = VectorAssembler(
        inputCols=["GiaDieuChinh", "GiaMoCua", "GiaDongCua", "GiaCaoNhat", "GiaThapNhat", "ThayDoi_value"],
        outputCol="features"
    )
    df_features = assembler.transform(df_sym).select("features", "percent_change").dropna(subset=["percent_change"])

    train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)

    rf = RandomForestRegressor(featuresCol="features", labelCol="percent_change", predictionCol="prediction")
    pipeline = Pipeline(stages=[rf])
    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)

    evaluator_rmse = RegressionEvaluator(labelCol="percent_change", predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol="percent_change", predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol="percent_change", predictionCol="prediction", metricName="r2")

    rmse = evaluator_rmse.evaluate(predictions)
    mae = evaluator_mae.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)

    print(f"RMSE: {rmse:.4f}, MAE: {mae:.4f}, R²: {r2:.4f}")

    results.append((sym, rmse, mae, r2))

results_df = spark.createDataFrame(results, ["Symbol", "RMSE", "MAE", "R2"])
results_df.show(truncate=False)

predictions.select("percent_change", "prediction").show(10)

