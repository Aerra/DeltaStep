import pygraph
from pygraph.algorithms.generators import generate
from pygraph.readwrite import markup
from time import time
from random import seed
import argparse

ROOT_DIR = "/home/asya/university/sqi/parallel_computing_p2/task/"
random_seed = int(time())

d = {} # distances from source node to others
B = {} # Bucket 
delta = 5

def new_graph(nodes=10, edges=18, wt_range=(1, 5)):
    seed(random_seed)
    return generate(num_nodes=nodes, num_edges=edges, directed=True, weight_range=wt_range)

def relax(node, x):
    OldBucket = []
    NewBucket = []
    if x < d[node]:
        if float(d[node]/delta) in B:
            OldBucket = B[float(d[node]/delta)]
        if float(x/delta) in B:
            NewBucket = B[float(x/delta)]
        if node in OldBucket:
            OldBucket.remove(node)
            B[float(d[node/delta])] = OldBucket
        if node not in NewBucket:
            NewBucket.append(node)
            B[float(x/delta)] = NewBucket
        d[node] = x

def relaxall(requests):
    for node in requests:
        relax(node, requests[node])

def findrequests(graph, Bucket, Edges):
    requests = {}
    for node in Bucket:
        node_neighbours = graph.neighbors(node)
        #print(f'Neighbours: {node_neighbours}')
        for edge in Edges:
            if edge[0] == node and edge[1] in node_neighbours:
                #print(f'From: {edge[0]}')
                #print(d[edge[0]])
                requests[edge[1]] = d[node] + graph.edge_weight(edge)
    return requests

def deltastepping(graph, args):
    LightEdges = []
    HeavyEdges = []
    for edge in graph.edges():
        if graph.edge_weight(edge) <= delta:
            LightEdges.append(edge)
        else:
            HeavyEdges.append(edge)

    print(f'LightEdges: {LightEdges}')
    print(f'HeavyEdges: {HeavyEdges}')
    for node in graph.nodes():
        d[node] = float("inf")
    
    relax(args.source, 0)

    cnt = 0
    while B:
        print(f'Cnt {cnt}')
        i = min(B.keys())
        Bucket = B[i]
        #Deleted = []
        #while i in B:
        Requests = findrequests(graph, Bucket, LightEdges)
        #Deleted.append(Bucket)
        del B[i]
        print(Requests)
        relaxall(Requests)
        #relaxall(findrequests(graph, Deleted, HeavyEdges))
        relaxall(findrequests(graph, Bucket, HeavyEdges))
        cnt+=1
    return

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='Delta-stepping algorithm')
    arg_parser.add_argument('--source', type=int, default=0, help="Source vertice")

    args = arg_parser.parse_args()
    graph = new_graph()
    #print(graph.neighbors(0))
    dotstr = markup.write(graph)
    print(dotstr)

    deltastepping(graph, args)

    print(d)
    print(B)
    #f = open(str(ROOT_DIR)+"test_graph.xml", "w")
    #f.write(dotstr)
    #f.close()