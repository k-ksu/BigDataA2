import os
import unicodedata

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("data preparation")
    .master("local")
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .getOrCreate()
)

os.makedirs("data", exist_ok=True)

df = spark.read.parquet("/m.parquet")
n = 1000
df = (
    df.select(["id", "title", "text"])
    .sample(fraction=100 * n / df.count(), seed=0)
    .limit(n)
)


def create_doc(row):
    raw_name = str(row["id"]) + "_" + row["title"]
    ascii_name = (
        unicodedata.normalize("NFKD", raw_name)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    filename = "data/" + sanitize_filename(ascii_name).replace(" ", "_") + ".txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(row["text"])


df.foreach(create_doc)

spark.stop()
