from pyspark import SparkConf, SparkContext, RDD

import itertools
import sys, csv
from operator import add
import time
import math

from collections import defaultdict


conf = SparkConf().setAppName("SON")
sc = SparkContext(conf=conf)

timeStart = time.time()
caseNo = sys.argv[1]
file_path = sys.argv[2]
support_threshold = int(sys.argv[3])

candidate_keys=[]
tuple_length=0

data =sc.textFile(file_path)  # type: RDD
header_file= data.first()


def generateCandidates(freqset, k):

   count = {}
   candidate = []

   for i in xrange(len(freqset)-1):
       for j in xrange(i+1,len(freqset)):
           a= set(freqset[i])
           b= set(freqset[j])
           c= a.union(b)
           if len(c)==k:
               candidate.append(tuple(sorted(c)))

      #x = tuple(sorted(set(i+j)))

   return candidate

def combi_count(combo, chunk, ps):

   res = []
   item = defaultdict(int)
   if combo:
       #li = [i for i in chunk if len(i) >= len(combo[0])]
       if len(chunk) < ps:
           yield res

       
       for movie in combo:
	   c_x = 0
           for l in chunk:
               x = set(movie)
               for i in movie:
                   if i in l:
                       x.remove(i)
               if not x:
                   c_x += 1
           if c_x >= ps:
               res.append(tuple(sorted(movie)))
       """for movie in combo:

           for l in chunk:
               if set(movie).issubset(set(l)):

                   item[tuple(sorted(movie))]+=1

                   #if x in item :
                       #item[x] += 1
                   if item[tuple(sorted(movie))] >= ps:
                       res.append(tuple(sorted(movie)))
                        break
       """
   yield res

def phase1_SON_MR(input_chunk, lent):

   list_inp = list(input_chunk)
   global candidate_keys, support_threshold,tuple_length
   ps = math.floor(support_threshold * float(len(list_inp)) / float(lent))


   candi_keys = combi_count(candidate_keys, list_inp, ps)  # type: defaultdict[Any, int]

   return candi_keys

def get_freq_set(keys, chunk):

   result =[]


   if keys:
       #chunk = [item for item in chunk if len(item) >= len(keys[0])]


       for tup in keys:
           c_x = 0
           for l in chunk:
               x = set(tup)
               for i in tup:
                   if i in l:
                       x.remove(i)
               if not x:
                   c_x += 1

           result.append(tuple((tuple(sorted(tup)),c_x)))
       """for tup in keys:
           c_x=0
           for l in chunk:

               if set(tup).issubset(set(l)):
                   x = tuple(sorted(tup))
                   c_x+=1

           result.append(tuple((x, c_x)))"""
   yield result

def phase2_SON_MR(input_chunk,s, candidateKeys):

   list_inp = list(input_chunk)
   #ps = math.floor(s * float(len(list_inp)) / float(len))
   final_list = get_freq_set(candidateKeys, list_inp)
   """for key, value in candidateSetFrequency.items():
       result.append(tuple((key, value)))"""
   return final_list



def get_freq_1(x):
   freq = []
   for key, value in x:
       # print "value: ", value
       if value >= support_threshold:
           freq.append(key)
   return freq

if __name__ == "__main__":


   #p =data.getNumPartitions()
   user_movie = data.filter(lambda x : x!= header_file)

   file = open('Shyamala_Sundararajan_SON_MovieLens.Small.case1-150.txt', 'w')
   if caseNo =="1" :
       user_movie =user_movie.map(lambda x : x.split(',')).map(lambda  y : (int(y[0]), int(y[1]) ))
   if caseNo == "2":
       user_movie = user_movie.map(lambda x : x.split(',')).map(lambda y : (int(y[1]), int(y[0]) ))

   print time.time() - timeStart


   user_movie_list = user_movie.groupByKey().map(lambda x: set(x[1]))
   user_movie_length = user_movie_list.count()

   usrMov_1 = user_movie_list.flatMap(lambda x: x).map(lambda y: (y, 1)).reduceByKey(add)
   final_usrMov_1 =usrMov_1.mapPartitions(lambda x : get_freq_1(x))
   freq_items = final_usrMov_1.collect()
   freq_items.sort()

   # final_userMov= usrMovRdd.filter(lambda x: x[1]>=support_threshold).map(lambda x: x[0]).persist()
   file.write(str(','.join(map(lambda x: '(' + str(x) + ')', freq_items))) + "\n" + "\n")

   tuple_length = 2
   candidate_keys = list(itertools.combinations(freq_items, 2))

   candidates = user_movie_list.mapPartitions \
       (lambda x: phase1_SON_MR(x, user_movie_length))

   candidate_keys = candidates.flatMap(lambda x: x).distinct().collect()

   NotFound =False

   while not NotFound :
       if tuple_length > 2:

         candidate_keys = generateCandidates(candidate_keys, tuple_length)

	 if not candidate_keys:
		break
         candidates=user_movie_list.filter(lambda x: len(x) >= len(candidate_keys[0])).mapPartitions\
           (lambda x : phase1_SON_MR(x,user_movie_length))

         candidate_keys = candidates.flatMap(lambda x : x).distinct().collect()


       frequent_itemsets=user_movie_list.mapPartitions\
           (lambda x : phase2_SON_MR(x,support_threshold, candidate_keys)).flatMap(lambda x: x).reduceByKey(add)\
           .filter(lambda x: x[1] >= support_threshold).map(lambda x: (x[0])).collect()
       frequent_itemsets.sort()


       if not frequent_itemsets:
           NotFound= True
           break
       else:
           tuple_length += 1
       item_sets = map(lambda x: str(x), sorted(frequent_itemsets))
       final_lst = str(','.join(item_sets))
       file.write(final_lst + "\n" + "\n")


       #for keys in frequent_itemsets:
        #   items.append(keys)

       candidate_keys =frequent_itemsets


   print  "Total Time Taken %f", (time.time() - timeStart)
   file.close()



