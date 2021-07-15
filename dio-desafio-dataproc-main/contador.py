import sys
import os
from pyspark import SparkContext, SparkConf

os.environ["PYSPARK_PYTHON"]="py"

if __name__ == "__main__":
    sc = SparkContext("local[*]", "PySpark Exemplo - Desafio Dataproc")
    words = sc.textFile("livro.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(
        lambda a, b: a + b).sortBy(lambda a: a[1], ascending=False)
    wordCounts.saveAsTextFile("resultado")
