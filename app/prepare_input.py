import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("prepare input data").master("local").getOrCreate()

sc = spark.sparkContext

rdd = sc.wholeTextFiles("/data")


def parse_document(file_tuple):
    filepath, content = file_tuple
    filename = os.path.basename(filepath)
    name_without_ext = filename.rsplit(".", 1)[0]
    parts = name_without_ext.split("_", 1)
    doc_id = parts[0]
    doc_title = parts[1].replace("_", " ") if len(parts) > 1 else ""
    clean_content = content.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    return doc_id + "\t" + doc_title + "\t" + clean_content


result_rdd = rdd.map(parse_document).coalesce(1)
result_rdd.saveAsTextFile("/input/data")

spark.stop()
