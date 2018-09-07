import sys,csv

from pyspark import SparkContext, SparkConf
def toCSVLine(data):
  return ','.join(str(d) for d in data)



if __name__ == "__main__":

  small_ratings_file = sys.argv[1]
  output_path = sys.argv[2]
  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Spark Count")
  sc = SparkContext(conf=conf)
  small_ratings_raw_data = sc.textFile(small_ratings_file)
  small_ratings_raw_data_header = small_ratings_raw_data.take(1)[0]

  small_ratings_data = small_ratings_raw_data.filter(lambda line: line != small_ratings_raw_data_header) \
    .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[1]), float(tokens[2]))).cache()

  movie_ratingprod = small_ratings_data.map(lambda x: (x[0], (x[1])))
  aTuple = (0,0.0) # As of Python3, you can't pass a literal sequence to a function.
  movie_sumrating = movie_ratingprod.aggregateByKey(aTuple, lambda a,b: (a[0] + b,    a[1] + 1),
                                       lambda a,b: (a[0] + b[0], a[1] + b[1]))
  print movie_sumrating.take(3)
  movie_avgrating = movie_sumrating.mapValues(lambda x: (x[0] / x[1])).sortByKey(True)
  output=movie_avgrating.collect()
  file = open(output_path , 'w')
  csvFile = csv.writer(file)
  header = ['movieID', 'rating_avg']
  csvFile.writerow(header)

  for elem in output:
    d = ['%s' % (elem[0]), '%s' % (elem[1])]
    csvFile.writerow(d)

  file.close()