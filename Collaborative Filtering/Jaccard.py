from pyspark import SparkConf, SparkContext, RDD

import itertools
import sys, csv
from operator import add
import time
import math
import random
import operator
from itertools import combinations


from collections import defaultdict

conf = SparkConf().setAppName("Jaccard")
sc = SparkContext(conf=conf)

timeStart = time.time()
file_path = sys.argv[1]
output_path = "Shyamala_Sundararajan_SimilarMovie_Jaccard.txt"

def signature(data, s):
    sig = data.map(lambda x: (int(x[0]), int(x[1])))
    sig = sig.map(lambda x: (x[0], (x[1], s[x[0]-1]))).map(
        lambda x: (x[1][0], x[1][1])).groupByKey().sortByKey()

    sig = sig.map(lambda x: (x[0], min_val(list(x[1]))))

    return sig

def min_val(li):

    l= list(li)
    l.sort()
    min_v = l[0]

    return min_v

def generateHash(ch_ma,data):
    c= 671
    a, b, m,n = random.sample(range(1, c-1), 4)
    h1=  ch_ma.map(lambda x: (a*x[0] + b) % c).collect()
    h2 = ch_ma.map(lambda x: (m*x[0] + n) % c).collect()
    s1 = signature(data, h1)
    s2 = signature(data, h2)
    sig_matrix = s1.join(s2)
    for l in range(10) :
        g= [(h1[i]+(l+1)*h2[i])%c for i in range(len(h1))]
        s = signature(data, g)
        sig_matrix = sig_matrix.join(s).map(lambda x: (x[0], make_list(x[1][0], x[1][1])))

    return sig_matrix

def make_list(tup, new):
    l = list(tup)
    l.append(new)

    return tuple(l)

def jac_sim(l1, l2):

    inter= len(list(set(l1).intersection(l2)))
    jacSim = float(inter) / float(len(l1) + len(l2) - inter)

    return jacSim

def candidatePairs(list1, list2 ):
    l1=list(list1)
    l2=list(list2)
    row= 2
    bands = 6

    for b in range(bands):
        sim = 0
        for i in range(row):
            if l1[i + b * row] == l2[i + b * row]:
                sim += 1
                if sim == row:
                    return True
            else:
                continue

    return False
if __name__ == "__main__":
    data =sc.textFile(file_path)
    header_file= data.first()
    data = data.filter(lambda x: x!= header_file).map(lambda x: x.split(','))

    char_matrix = data.map(lambda x: (int(x[0]),(int(x[1]),1))).groupByKey().sortByKey(True)
    bandhash =generateHash(char_matrix,data).sortByKey(True).collect()
    res = []
    print "Total time  start candidate pair%f sec" % (time.time() - timeStart)

    for i in range(len(bandhash) - 1):
        for j in range(i + 1, len(bandhash)):

            if candidatePairs(bandhash[i][1], bandhash[j][1]):
                res.append((bandhash[i][0], bandhash[j][0]))



    print "Total time end candidate pair %f sec" % (time.time() - timeStart)

    pairs=[]
    mov_matrix = data.map(lambda x: (int(x[1]), int(x[0]))).groupByKey().sortByKey(True).collect()
    dic = dict(mov_matrix)
    print "Total time  start jaccard %f sec" % (time.time() - timeStart)
    pairs = sc.parallelize(res).map(lambda x: (x[0], x[1], jac_sim(dic[x[0]], dic[x[1]]))).sortByKey(ascending=True)\
        .filter(lambda x : x[2] >=0.5)

    print "Total time end of jaccard %f sec" % (time.time() - timeStart)

    file = open(output_path, 'w')


    for s in pairs.collect():
        file.write("%s,%s,%s" % (s[0], s[1], s[2]) + "\n")
    file.close()


    print "Total time %f sec" % (time.time() - timeStart)
    """predict_pairs = pairs.map(lambda x: (x[0], x[1]))

    gr_t = sc.textFile("SimilarMovies.GroundTruth.05.csv", None, False).map(lambda x: x.split(','))
    gr_t = gr_t.map(lambda x: (int(x[0]),int(x[1])))
    print "Total time %f sec" % (time.time() - timeStart)
    tp = gr_t.intersection(predict_pairs).count()
    fp = predict_pairs.subtract(gr_t).count()
    fn = gr_t.subtract(predict_pairs).count()
    precision = float(tp) / float(tp + fp)
    recall = float(tp) / float(tp + fn)

    print "Precision ", precision
    print "Recall: ",recall
    """
    print "Total time %f sec" % (time.time() - timeStart)
