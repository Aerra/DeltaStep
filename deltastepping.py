import pygraph
from pygraph.classes.graph import graph
from pygraph.classes.digraph import digraph
from pygraph.algorithms.generators import generate
from pygraph.readwrite import markup
from time import time
from random import seed
import argparse
import numpy as np

#ROOT_DIR = "/home/asya/university/sqi/parallel_computing_p2/task/"
random_seed = int(time())

d = {} # distances from source node to others
B = {} # Bucket
delta = 0.01
debug = False

def read_graph(filename):
    file = open(filename, "rb")
    if not file:
        ValueError("Can't open file")

    nodes = np.frombuffer(file.read(4), np.uint32)[0]
    arity = np.frombuffer(file.read(8), np.uint64)[0]
    directed = np.frombuffer(file.read(1), np.bool8)[0]
    _ = np.frombuffer(file.read(1), np.uint8)[0]
    rowIndices = np.frombuffer(file.read(8 * (nodes + 1)), np.uint64)
    nEdges = arity * nodes
    endV = np.frombuffer(file.read(4 * int(nEdges)), np.uint32)
    nRoots = np.frombuffer(file.read(4), np.uint32)[0]
    _ = np.frombuffer(file.read(4 * nRoots), np.uint32)
    _ = np.frombuffer(file.read(8 * int(nRoots)), np.uint64)
    weights = np.frombuffer(file.read(8 * int(nEdges)), np.float64)

    #print(f'Nodes {nodes}')
    #print(f'arity {arity}')
    #print(f'directed {directed}')
    #print(f'align {align}')
    #print(f'RowIndices {rowIndices}')
    #print(f'endV {endV}')
    #print(f'nEdges {nEdges}')
    #print(f'weights {weights}')

    if directed:
        read_graph = digraph()
    else:
        read_graph = graph()

    for i in range(nodes):
        read_graph.add_node(i)

    for i in range(nodes):
        # print(f'I {i}')
        for j in range(rowIndices[i], rowIndices[i+1]):
            # print(f'Edge ({i}, {endV[j]})')
            edge = (i, endV[j])
            if read_graph.has_edge(edge):
                edge_weight = read_graph.edge_weight(edge)
                if edge_weight > weights[j]:
                    read_graph.set_edge_weight(edge, weights[j])
            else:
                read_graph.add_edge(edge, wt=weights[j])

    return read_graph

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
            B[float(d[node]/delta)] = OldBucket
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

    if debug:
        print(f'LightEdges: {LightEdges}')
        print(f'HeavyEdges: {HeavyEdges}')
    for node in graph.nodes():
        d[node] = float("inf")
    
    relax(args.source, 0)
    while B:
        i = min(B.keys())
        Bucket = B[i]
        Requests = findrequests(graph, Bucket, LightEdges)
        del B[i]
        #print(Requests)
        relaxall(Requests)
        relaxall(findrequests(graph, Bucket, HeavyEdges))
    return

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='Delta-stepping algorithm')
    arg_parser.add_argument('--source', type=int, default=0, help="Source vertice")
    arg_parser.add_argument('--delta', type=float, default=5.0, help="Delta step")
    arg_parser.add_argument('--input', type=str, default=None, help="Binary file with graph information")
    arg_parser.add_argument('--nodes', type=int, default=10, help="Generate random graph with 'nodes' nodes")
    arg_parser.add_argument('--edges', type=int, default=20, help="Generate random graph with 'edges' edges")
    arg_parser.add_argument('--weight_min', type=int, default=1, help="Generate random graph with mininum edge weight 'weight_min'")
    arg_parser.add_argument('--weight_max', type=int, default=5, help="Generate random graph with maximum edge weight 'weight_max'")
    arg_parser.add_argument('--debug', type=bool, default=False, help="Debug mode")
    arg_parser.add_argument('--out', type=str, default=None, help="Out file")

    args = arg_parser.parse_args()
    delta = args.delta
    debug = args.debug

    if args.input == None:
        graph = new_graph(nodes=args.nodes, edges=args.edges, wt_range=(args.weight_min, args.weight_max))
    else:
        graph = read_graph(args.input)

    if debug:
        dotstr = markup.write(graph)
        print(dotstr)

    deltastepping(graph, args)

    if args.out != None:
        f = open(args.out, "w")
        for key in d.keys():
            f.write(f'{key}: {d[key]}\n')
        f.close()
    else:
        print(d)