import os, sys, operator
import networkx as nx
from collections import defaultdict
from pyspark import SparkContext
import time
import scipy as sp
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


def modularity():

    c=0.0
    comm =dict()

    for i in range(0, len(conn_comp)):
        for node in conn_comp[i]:
            comm[node]=i

    for i in nodes:
        for j in nodes:
            if comm[i] == comm[j]:
                c+= edg_dic[i][j]
    Q =  c / float(2 *m)
    return Q

def adj_mat(edges):

        vertices=len(nodes)
        for i in range(1, vertices + 1):
            Aij[i] = dict()
            for j in range(1, vertices + 1):
                Aij[i][j] = 0
        for i in nodes:
            for j in graph[i]:

                Aij[i][j] = 1

def gintama():
    maxQ=0
    for i in nodes:
        if i not in edg_dic:
            edg_dic[i]= dict()
        for j in nodes:
            ki = degree[i]
            kj = degree[j]
            edg_dic[i][j]=float(Aij[i][j])-float(ki*kj)/float(2*m)

            maxQ+=edg_dic[i][j]
    return maxQ

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
print "Time taken to finish btw", time.time()-START_TIME


new_bet_dic.update((k, (float(round(v/0.2,2)/10))) for k, v in new_bet_dic.items())

max_bet= sorted(new_bet_dic,key=new_bet_dic.get, reverse=True)
print "Time taken to reach upated btw", time.time()-START_TIME
grps= list()
m = graph.number_of_edges() # value of m
output = open("Shyamala_Sundararajan_Community.txt", 'w')

degree= dict()
for i in graph.nodes():
    degree[i]= graph.degree(i)

nodes= graph.nodes()

Aij = dict()
adj_mat(edges)
edg_dic=dict()

M=gintama()

M=float(M)/float(2*m)
ed= 15000
print "inital normalization",time.time()-START_TIME
l1=[]
for i in range(0,ed):
    l1.append(max_bet[i])
graph.remove_edges_from(l1)
initalCom=len(list(nx.connected_components(graph)))

for i in  range(ed,len(max_bet)-10,10):

    remove_list=[]
    for j in range(i,i+10):
        remove_list.append(max_bet[j])

    graph.remove_edges_from(remove_list)
    conn_comp = list(nx.connected_components(graph))
    l = len(conn_comp)

    if graph.number_of_edges() == 0 or graph.number_of_edges()<5000:

        break
    elif (l<=initalCom):
        continue

    else:
        initalCom = l
        Q= modularity()
        if Q>M:

          M=Q
          grps= conn_comp

        else:
            continue

a=[]
print M,len(grps)
for i in grps:
    a.append(sorted(list(i)))
for j in sorted(a):
    output.write(str(j))
    output.write("\n")

output.close()


print "Time taken ", time.time()-START_TIME


