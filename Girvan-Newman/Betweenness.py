import os, sys, operator
import networkx as nx
from collections import defaultdict
from pyspark import SparkContext
import time
import math
from operator import add




def createGraph( nodes, usr_mov):
    print "time taken crete Graph begin is ", time.time() - START_TIME
    edges= []
    for x in nodes:
        for y in nodes:
            if(x!=y):
             list1= set(usr_mov[x])
             list2 = set(usr_mov[y])
             if len(list(list1.intersection(list2))) >= 9:
                 edges.append((x,y))
    print "time taken crete Graph is ", time.time() - START_TIME
    return edges


def BFS(dic,nodelist):
    for node in nodelist:

        q = list()
        visited = dict()
        credits = dict()
        parent = dict()
        path =[]
        q.append(node)
        credits[node] = 0.0
        visited[node] = True
        parent[node]=[]


        """for v in graph[node]:
            parent[v] = [node, ]
            visited[v] = True
            credits[v] = 1
            q.append(v)
            path.append(v)
        """


        while q:
            v = q.pop(0)
            path.append(v)
            visited[v] = True
            for child in graph[v]:
                if child not in visited:
                    if child not in credits:
                        q.append(child)
                        credits[child] =  credits[v]+ 1
                        parent[child]=[v,]

                    else:
                        if credits[child] > credits[v]:
                            credits[child] =credits[v]+1
                            parent[child].append(v)

        credit_edge = dict.fromkeys(path, 1)

        while path:
            v = path.pop()
            if len(parent[v])==0:
                continue
            c = (1.0  / len(parent[v]))#credits[v]

            for p in parent[v]:
                child = credits[v] * c
                e = tuple(sorted((p, v)))

                dic[e] += child

                credit_edge[p] += child

    return dic




START_TIME = time.time()
sc = SparkContext()
final_result = []
movie = sc.textFile(sys.argv[1]).map(lambda line: line.split(","))
header = movie.first()
movie = movie.filter(lambda line: line != header)

data = movie.map(lambda x : (int(x[0]),int(x[1]))).groupByKey().sortByKey().map(lambda x : (int(x[0]), list(x[1])))\
    .filter(lambda x : (len(x[1])>=9))

user_movies = data.collectAsMap()


nodes = movie.map(lambda x : (int(x[0]),1)).distinct().sortByKey().map(lambda x : x[0]).collect()
print "time taken nodes is ", time.time() - START_TIME
edges= createGraph(nodes, user_movies)
#connected_users=sc.parallelize(edges).groupByKey().sortByKey()\
#                            .map(lambda x : (int(x[0]),list(x[1]))).collectAsMap()

graph= nx.Graph()
graph.add_edges_from(edges)
bet_dic =defaultdict.fromkeys(graph.edges(), 0.0)
new_bet_dic=BFS(bet_dic,list(graph.nodes()))

output = open("Shyamala_Sundararajan_Betweenness.txt", 'w')
sort_dic = sorted(new_bet_dic.keys())
betweenness = []
for i in sort_dic:
    betweenness.append((i[0], i[1],(float(round(new_bet_dic[i]/0.2,1)/10))))
betweenness = sorted(betweenness, key = lambda x: (x[0], x[1]))

for i in betweenness:
    output.write("("+str(i[0])+","+str(i[1])+","+str(i[2])+")")
    output.write("\n")
output.close()
print "Time taken ", time.time()-START_TIME


