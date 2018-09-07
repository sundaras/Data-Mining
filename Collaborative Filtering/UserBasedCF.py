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

output_path = "Shyamala_Sundararajan_UserBasedCF.txt"
user_movies =defaultdict(dict)
def pearson(user1, user2):
    corated = [i for i in user1 if i in user2]


    if len(corated) == 0:
        return 0

    mean_1 = sum([user1[i] for i in corated]) / len(corated)
    mean_2 = sum([user2[i] for i in corated]) / len(corated)

    subtracted_mean_1 = [user1[i] - mean_1 for i in corated]
    subtracted_mean_2 = [user2[i] - mean_2 for i in corated]

    numer = sum([a * b for a, b in list(zip(subtracted_mean_1, subtracted_mean_2))])

    user1_squared = [i*i for i in subtracted_mean_1]
    user2_squared = [i * i for i in subtracted_mean_2]

    denom= math.sqrt(sum(user1_squared) * sum(user2_squared))

    if denom == 0:
        return 0
    return numer / denom

def get_sim_users(user1,knn):

    sim= []
    for users in user_movies.keys():
        if users!=user1:
            pc=pearson(user_movies[user1], user_movies[users])
            if sim!=0:
             sim.append((pc,users))


    sim.sort(reverse=True)

    if knn != None:
        sim = sim[0:knn]

    return sim

def userModel(knn):
    users ={}
    for user in user_movies.keys():

        users[user] = get_sim_users(user,knn)
    return users



def mis_usr_ratings(user, movie):
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
        neighbors= model[user]
        valid_neighbors= valid_neigh(movie,neighbors)
        num=0.0
        denom=0.0

        if valid_neighbors:
            for n in valid_neighbors:

                n_usr_id= n[1]
                n_sim= n[0]

                n_usr_rating= user_movies[n_usr_id][movie]

                num += (n_usr_rating - avg_user(user_movies[n_usr_id], movie)) * n_sim
                denom += abs(n_sim)



            x=abs(avg_user(user_movies[user],movie) + (num/denom))

            return x
        else:
            return mis_usr_ratings(user, movie)
    except KeyError:

        rating = mis_usr_ratings(user, movie)
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

        user_movies[user][movie] = float(rating)


    neigh=6
    model= userModel(neigh)

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


