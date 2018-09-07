from pyspark import SparkConf, SparkContext, RDD

import itertools
import sys, csv
from operator import add
import time
import math
import random
import operator
from itertools import combinations
from pyspark.mllib.recommendation import Rating, ALS
from collections import defaultdict

conf = SparkConf().setAppName("ModelBasedCF")
sc = SparkContext(conf=conf)

timeStart = time.time()
file1 = sys.argv[1]
file2= sys.argv[2]
output_path = "Shyamala_Sundararajan_ModelBasedCF_Small.txt"
if __name__ == "__main__":
    train =sc.textFile(file1)
    h1= train.first()
    test = sc.textFile(file2)
    h2= test.first()

    test = test.filter(lambda x : x != h2).map(lambda x: x.split(','))

    test_data = test.map(lambda x : ((int(x[0]), int(x[1])),1)).sortByKey()

    train = train.filter(lambda x : x != h1).map(lambda x: x.split(','))
    train_data = train.map(lambda x : ( (int(x[0]), int(x[1])), float(x[2]))).sortByKey()
    new_train = train_data.subtractByKey(test_data)\
        .map(lambda x : Rating(int(x[0][0]), int(x[0][1]), float(x[1])))


    rank = 4
    numIterations = 10
    model = ALS.train(new_train, rank, numIterations, 0.06)

    test_user= test_data.map(lambda x : (int(x[0][0]), int(x[0][1])))
    predictions =model.predictAll(test_user).map(lambda r : ((r[0], r[1]), r[2]))
    #predicted_users= predictions.map(lambda r : (r[0],r[1],r[2]))

    avg_ratings = predictions.map(lambda p :  (p[0][0], p[1]))

    sum = avg_ratings.combineByKey(lambda v: (v, 1),
                             lambda x, value: (x[0] + value, x[1] + 1),
                             lambda x, y: (x[0] + y[0], x[1] + y[1]))
    avg = sum.map(lambda (x, (sum, count)): (x, sum / count)).sortByKey()

    #avg= avg_ratings.reduceByKey(lambda x, y: (x[1][0] + y[1][0], x[1][1] + y[1][1]))\
     #         .mapValues(lambda x :1.0 * x[0] / x[1]).sortByKey()


    #Handling missing users
    mis_users = test_data.subtractByKey(predictions).map(lambda r : (int(r[0][0]), int(r[0][1]))).join(avg)\
                                                    .map(lambda x :  (int(x[0]), int(x[1][0]), int(x[1][1])))

    ratesAndPreds = train_data.map(lambda r: ((r[0][0], r[0][1]), r[1]))\
                                     .join(predictions.union(mis_users)).sortBy(lambda x : (x[0][0], x[0][1]))
    print ratesAndPreds.take(3)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()
    RMSE = math.sqrt(MSE)
    diff=  ratesAndPreds.map(lambda r : abs(r[1][0]- r[1][1]))

    file = open(output_path, 'w')
    file.write("User_Id,Movie_Id,Pred_Ratings")
    file.write("\n")
    for s in ratesAndPreds.collect():
        file.write("%s,%s,%s" % (s[0][0], s[0][1], s[1][1]) + "\n")
    file.close()

    lv1 = diff.filter(lambda x : x>=0 and x<1).count()
    lv2 = diff.filter(lambda x: x >= 1 and x < 2).count()
    lv3 = diff.filter(lambda x: x >= 2 and x < 3).count()
    lv4 = diff.filter(lambda x: x >= 3 and x < 4).count()
    lv5 = diff.filter(lambda x: x >= 4 ).count()

    print "Total time taken is %f " %(time.time()-timeStart)
    print ">=0 and <1: %d" % (lv1)
    print ">=1 and <2: %d" % lv2
    print ">=2 and <3: %d" %lv3
    print ">=3 and <4: %d" %lv4
    print ">=4: %d " %lv5
    print "RMSE = %f" % RMSE


