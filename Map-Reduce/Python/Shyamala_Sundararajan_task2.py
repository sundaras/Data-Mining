import sys, csv

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def toCSVLine(data):
    return ','.join(str(d) for d in data)


if __name__ == "__main__":

    small_ratings_file = sys.argv[1]
    small_tags_file = sys.argv[2]
    output_path = sys.argv[3]
    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Spark Count")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    #rdd1 = sc.textFile(small_ratings_file)
    #small_ratings_raw_data_header = rdd1.take(1)[0]
    #rdd2 = sc.textFile(small_tags_file, None, False)
    #small_tags_raw_data_header = rdd2.take(1)[0]

    r1 = spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").csv(small_ratings_file)

    r2 = spark.read.option("header", "true").option("charset", "utf-8").option("quote", "\"").option("escape", "\"").csv(small_tags_file)

    dftoRdd1 = r1.rdd
    dftoRdd2 = r2.rdd

    small_ratings_data = dftoRdd1.map(lambda tokens: (int(tokens[1]), float(tokens[2]))).cache()

    tagsRDD = dftoRdd2.map(lambda tokens: (int(tokens[1]), str(tokens[2]).lower())).sortByKey(True).cache()

    movie_ratingprod = small_ratings_data.map(lambda x: (x[0], (x[1])))
    movie_tags = tagsRDD.map(lambda x: (x[0], (x[1])))

    aTuple = (0, 0.0)
    movie_sumrating = movie_ratingprod.aggregateByKey(aTuple, lambda a, b: (a[0] + b, a[1] + 1),
                                                      lambda a, b: (a[0] + b[0], a[1] + b[1]))

    movie_avgrating = movie_sumrating.mapValues(lambda x: (x[0] / x[1])).sortByKey(True)

    newrdd = movie_tags.join(movie_avgrating)
    newrdd = newrdd.map(lambda a: (a[1][0], a[1][1]))
    bTuple = (0, 0.0)
    movie_sumtagrating = newrdd.aggregateByKey(aTuple, lambda a, b: (a[0] + b, a[1] + 1),
                                               lambda a, b: (a[0] + b[0], a[1] + b[1]))

    movie_tagavg = movie_sumtagrating.map(lambda x: (x[0], x[1][0] / x[1][1])).sortByKey(False)
    output = movie_tagavg.collect()
    file = open(output_path, 'w')
    csvFile = csv.writer(file)
    header = ['tag', 'rating_avg']
    csvFile.writerow(header)

    for elem in output:
        d = ['%s' % (elem[0]), '%s' % (elem[1])]
        csvFile.writerow(d)

    file.close()

    # newRdd = movie_ratingprod.join(movie_tags, movie_ratingprod.a_id == b.b_id)
    # newRdd=small_ratings_data.join(tagsRDD)
    # newRdd= newRdd.map(lambda tokens: (str(tokens[1), (tokens[2]))).cache()
