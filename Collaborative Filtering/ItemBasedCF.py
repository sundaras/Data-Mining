from pyspark import SparkConf, SparkContext, RDD
import itertools
import sys, csv
from operator import add
import time
import math

from collections import defaultdict

conf = SparkConf().setAppName("UserBasedCF")
sc = SparkContext(conf=conf)

timeStart = time.time()
file1 = sys.argv[1]
file2= sys.argv[2]

output_path = "Shyamala_Sundararajan_ItemBasedCF.txt"
lsh_file = "Shyamala_Sundararajan_SimilarMovie_Jaccard.txt"
movie_users =defaultdict(dict)
user_movies= defaultdict(dict)
def LSH_jac(file_name):

    lsh = sc.textFile(lsh_file)
    h1 = lsh.first()
    lsh = lsh.filter(lambda x: x != h1).map(lambda x: x.split(','))
    lsh_data = lsh.map(lambda x: ((int(x[0]), int(x[1])), float(x[2]))).sortByKey()
    lsh_m= defaultdict(dict)
    for items in lsh_data.collect():
        movie1=items[0][0]
        movie2=items[0][1]
        sim=items[1]

        lsh_m[movie1][movie2] = float(sim)

    return lsh_m

def pearson(movie1, movie2):
    corated = [i for i in movie1 if i in movie2]


    if len(corated) == 0:
        return 0

    mean_1 = sum([movie1[i] for i in corated]) / len(corated)
    mean_2 = sum([movie2[i] for i in corated]) / len(corated)

    subtracted_mean_1 = [movie1[i] - mean_1 for i in corated]
    subtracted_mean_2 = [movie2[i] - mean_2 for i in corated]

    numer = sum([a * b for a, b in list(zip(subtracted_mean_1, subtracted_mean_2))])

    user1_squared = [i*i for i in subtracted_mean_1]
    user2_squared = [i * i for i in subtracted_mean_2]

    denom= math.sqrt(sum(user1_squared) * sum(user2_squared))

    if denom == 0:
        return 0
    return numer / denom

def get_sim_movies(movie1,knn,similar):

    sim= []
    try:
        mov_items= similar[movie1]
        for movie in mov_items.keys():

            coeff=mov_items[movie]

            sim.append((coeff,movie))

    except KeyError:
        for movies in movie_users.keys():
            if movies != movie1:
                pc = pearson(movie_users[movie1], movie_users[movies])
                if sim != 0:
                    sim.append((pc, movies))

    sim.sort(reverse=True)

    if knn != None:
        sim = sim[0:knn]

    return sim

def movieModel(knn,similar):
    movies ={}
    for movie in movie_users.keys():

        movies[movie] = get_sim_movies(movie,knn,similar)
    return movies



def mis_usr_ratings(user):
    if user_movies[user]:
        mean = sum([user_movies[user][m] for m in user_movies[user]]) / len(user_movies[user])

        return mean
    else:

        return -1

def avg_user(user,movie):

    sums= [value for key,value in user.items() if movie !=key ]

    return sum(sums)/len(sums)

def valid_neigh(movie, neigh):
        result = []
        for n in neigh:
            n_id = n[1]

            if movie in user_movies[n_id].keys():
                result.append(n)
        return result

def ratesAndPreds(user, movie, model):
    try:
        neighbors= model[movie]
        #valid_neighbors= valid_neigh(movie,neighbors)
        num=0.0
        denom=0.0

        if neighbors:
            for n in neighbors:

                n_mov_id= n[1]
                n_sim= n[0]
                try:

                    num += user_movies[user][n_mov_id] * n_sim
                except KeyError:
                    num +=  avg_user(user_movies[user],movie) * n_sim
                denom += abs(n_sim)
            x=abs(num/denom)


            return x
        else:
            return mis_usr_ratings(user)
    except KeyError:

        rating = mis_usr_ratings(user)
        return rating



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
        .map(lambda x : ((int(x[0][0]), int(x[0][1])), float(x[1])))

    for items in new_train.collect():
        user=items[0][0]
        movie=items[0][1]
        rating=items[1]
        user_movies[user][movie] =float(rating)
        movie_users[movie][user] = float(rating)

    similar= LSH_jac(lsh_file)

    neigh=4
    model= movieModel(neigh,similar)

    predRates = defaultdict(dict)
    for items in test_data.collect():
        p_coeff = ratesAndPreds(items[0][0], items[0][1], model)
        user =items[0][0]
        movie=items[0][1]
        predRates[user][movie] = float(p_coeff)


    file = open(output_path, 'w')
    file.write("User_Id,Movie_Id,Pred_Ratings")
    file.write("\n")

    rp_rdd = test_data.map(lambda ((user, movie),r): ((user, movie), predRates[user][movie])).sortByKey()
    for s in rp_rdd.collect():
        file.write("%s,%s,%s" % (s[0][0], s[0][1], s[1]) + "\n")
    file.close()

    final = train_data.map(lambda r: ((r[0][0], r[0][1]), r[1])) \
        .join(rp_rdd).sortBy(lambda x: (x[0][0], x[0][1]))
    print final.take(3)
    MSE = final.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()
    RMSE = math.sqrt(MSE)
    diff = final.map(lambda r: abs(r[1][0] - r[1][1]))

    lv1 = diff.filter(lambda x: x >= 0 and x < 1).count()
    lv2 = diff.filter(lambda x: x >= 1 and x < 2).count()
    lv3 = diff.filter(lambda x: x >= 2 and x < 3).count()
    lv4 = diff.filter(lambda x: x >= 3 and x < 4).count()
    lv5 = diff.filter(lambda x: x >= 4).count()

    print "Total time taken is %f " % (time.time() - timeStart)
    print ">=0 and <1: %d" % (lv1)
    print ">=1 and <2: %d" % lv2
    print ">=2 and <3: %d" % lv3
    print ">=3 and <4: %d" % lv4
    print ">=4: %d " % lv5
    print "RMSE = %f" % RMSE


