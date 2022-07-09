import logging
import pandas as pd
from typing import Optional
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def demo():
    # as shown in Quick Start
    logFile = "/home/nwind/miniconda3/envs/pyspark3.3/lib/python3.7/site-packages/pyspark/find_spark_home.py"
    spark = SparkSession.builder.appName("SparkOneApp (Linux; PyCharm)").getOrCreate()
    #spark.sparkContext.setLogLevel(logging.DEBUG)

    logData = spark.read.text(logFile).cache()

    numAs = logData.filter(logData.value.contains("a")).count()

    print(f"Line with a: {numAs}")


def read_test_files(spark, prefix: str, school: Optional[str] = "Great Neck") -> pd.DataFrame:
    rolls = [
        "Achievement Roll",
        "Honor Roll",
        "Dist Honor Roll"
    ]

    dfs = []
    for roll in rolls:
        fn = os.path.dirname(__file__) + "/data/" + f"{prefix}_{roll}.csv"
        df = pd.read_csv(fn, skiprows=[0, 1, 2])

        df["Type"] = roll
        dfs.append(df)

    df = pd.concat(dfs)

    ddf = spark.createDataFrame(df)

    ddf.filter(col("SchoolName").contains(school)).show(truncate=False)

    return ddf
    #
    # if school is not None:
    #     df = df[df["SchoolName"].str.contains(school)]
    #
    # df = df.drop_duplicates(subset=["score", "First_initial", "lastname", "grade"], keep="last")\
    #     .sort_values(by=["score", "grade"], ascending=[False, True])
    #
    # return df


if __name__ == "__main__":
    print(pyspark.__file__)
    spark = SparkSession.builder.appName("SparkOneApp (Linux; PyCharm)").getOrCreate()
    tests = ["2021Fall_AMC10A", "2021Fall_AMC10B", "2021_AMC10A", "2021_AMC10B"]

    # demo()
    read_test_files(spark, "2021Fall_AMC10A", "Great Neck")
