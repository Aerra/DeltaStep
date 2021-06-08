from os import fdatasync
from mpi4py import MPI
import argparse
import numpy as np
from numpy.core.fromnumeric import take
from numpy.lib.utils import source
#from pygraph.classes.graph import graph

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
nproc = comm.Get_size()

delta = 0.01
d = {}
B = {}

print(f'Rank: {rank}, nproc: {nproc}')

def read_graph(filename):
    if rank == 0:
        file = open(filename, "rb")
        if not file:
            ValueError("Can't open file")

        nodes = np.frombuffer(file.read(4), np.uint32)[0]
        arity = np.frombuffer(file.read(8), np.uint64)[0]
        directed = np.frombuffer(file.read(1), np.bool8)[0]
        _ = np.frombuffer(file.read(1), np.uint8)[0]
        nEdges = int(arity * nodes)
        rowIndices = np.frombuffer(file.read(8 * (nodes + 1)), np.uint64)
        endV = np.frombuffer(file.read(4 * nEdges), np.uint32)
        nRoots = np.frombuffer(file.read(4), np.uint32)[0]
        _ = np.frombuffer(file.read(4 * nRoots), np.uint32)
        _ = np.frombuffer(file.read(8 * nRoots), np.uint64)
        weights = np.frombuffer(file.read(8 * nEdges), np.float64)
    
        nodesonproc = nodes // (nproc - 1)
        nodesprocLast = nodesonproc + nodes % (nproc - 1)
        #rowIndicesonproc = (nodes + 1) // (nproc - 1)
        rowIndicesonproc = nodes // (nproc - 1)
        rowIndicesoffset = 0
        nodesoffset = 0
        for i in range(1, nproc):
            #print(f'i = {i}')
            comm.send(nodes, i)
            comm.send(nEdges, i)
            if i != nproc - 1:
                comm.send(nodesoffset, i)
                comm.send(nodesonproc, i)
                comm.send(rowIndices[rowIndicesoffset:(rowIndicesoffset+rowIndicesonproc+1)], i)
                comm.send(endV[rowIndices[rowIndicesoffset]:rowIndices[rowIndicesoffset+rowIndicesonproc+1]], i)
                comm.send(weights[rowIndices[rowIndicesoffset]:rowIndices[rowIndicesoffset+rowIndicesonproc+1]], i)
            else:
                comm.send(nodesoffset, i)
                comm.send(nodesprocLast, i)
                comm.send(rowIndices[rowIndicesoffset:(nodes+1)], i)
                comm.send(endV[rowIndices[rowIndicesoffset]:(rowIndices[nodes])], i)
                comm.send(weights[rowIndices[rowIndicesoffset]:(rowIndices[nodes])], i)

            rowIndicesoffset += rowIndicesonproc
            nodesoffset += nodesonproc
    else:
        nodesall = comm.recv(source=0)
        #print(f'rank = {rank}, nodes = {nodesall}')        
        edgesall = comm.recv(source=0)
        #print(f'rank = {rank}, nodes = {edgesall}')
        nodesoffset = comm.recv(source=0)
        #print(f'rank = {rank}, nodesoffset = {nodesoffset}')        
        nodes = comm.recv(source = 0)
        #print(f'rank = {rank}, nodes = {nodes}')
        rowIndices = comm.recv(source=0)
        #print(f'rank = {rank}, rowIndices = {rowIndices}')
        endV = comm.recv(source=0)
        #print(f'rank = {rank}, endV = {endV}')
        weights = comm.recv(source=0)
        #print(f'rank = {rank}, weights = {weigths}')

    if rank == 0:
        read_graph = {
            "nodes" : nodes,
            "nEdges": nEdges,
            "rowIndices": rowIndices,
            "endV": endV,
            "weights": weights
        }
    else:
        read_graph = {
            "nodesAll" : nodesall,
            "edgesAll" : edgesall,
            "nodesOffset": nodesoffset,
            "nodesLocal": nodes,
            "rowIndicesLocal": rowIndices,
            "endVLocal": endV,
            "weightsLocal": weights
        }

    return read_graph

def update_graph_info(graph):
    graph["g2lnodes"] = {}
    graph["l2gnodes"] = []
    graph["l2gendV"] = []
    graph["g2lendV"] = []

    #if rank == 3:
    #    print(f'{rank}: graph: {graph}')

    for i in range(graph["nodesLocal"]):
        globalnode = i + graph["nodesOffset"]
        graph["g2lnodes"][int(globalnode)] = i
        graph["l2gnodes"].append(globalnode)
        #if rank == 3:
        #    print(f'{rank}: {i}, {graph["rowIndicesLocal"]}')
        for j in range(graph["rowIndicesLocal"][i], graph["rowIndicesLocal"][i+1]):
            endvlocalidx = j - graph["rowIndicesLocal"][0]
            endVGlobalValue = graph["endVLocal"][int(endvlocalidx)]
            if endVGlobalValue not in graph["g2lnodes"]:
                graph["g2lnodes"][endVGlobalValue] = graph["nodesLocal"] + len(graph["l2gendV"])
                #graph["l2gnodes"][graph["nodesLocal"] + len(graph["l2gendV"])] = endVGlobalValue
                graph["g2lendV"][endVGlobalValue] = len(graph["l2gendV"])
                graph["l2gendV"].append(endVGlobalValue)

            endVLocalValue = graph["endVLocal"][int(endvlocalidx)] - graph["nodesOffset"]
            if endVLocalValue < 0 or endVLocalValue >= graph["nodesOffset"]:
                endVLocalValue = -1
            graph["g2lendV"].append(int(endVLocalValue))

    return graph

def exchange():
    # first sync
    if rank == 0:
        for i in range(1, nproc):
            comm.send((f'sync1 {i}'), dest=i, tag = 1)
    else:
        comm.recv(source=0, tag = 1)

    # first half of exchange
    if rank != 0:
        if (rank % 2 == 0):
            for i in range (1, nproc, 2):
                data = 10 + i
                comm.send(data, dest=i, tag = 2)
        else:
            for i in range (2, nproc, 2):
                data = comm.recv(source=i, tag= 2)
        comm.send('sync', dest=0, tag= 3)
    else:
        for i in range(1, nproc):
            comm.recv(source=i, tag= 3)

    # second sync
    if rank == 0:
        for i in range(1, nproc):
            comm.send(i, dest=i, tag= 4)
    else:
        comm.recv(source=0, tag= 4)
    
    # invert exchange    
    if rank != 0:
        if (rank % 2 == 1):
            for i in range (2, nproc, 2):
                data = 20 + i + rank
                comm.send(data, dest=i, tag = 5)
        else:
            for i in range (1, nproc, 2):
                comm.recv(source=i, tag= 5)
        comm.send('sync', dest=0, tag= 6)
    else:
        for i in range(1, nproc):
            comm.recv(source=i, tag= 6)

    # finish sync
    if rank == 0:
        for i in range(1, nproc):
            comm.send(i, dest=i, tag = 7)
    else:
        data = comm.recv(source=0, tag = 7)    

    return

def deltastepping(graph):
    for i in range(len(graph["g2lnodes"].keys())):
        node = graph[""]
        d[i] = float("inf")

    return graph

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Parallel DeltaStepping algorithm")
    arg_parser.add_argument('--input', type=str, default=None, help="Binary file with graph information")
    arg_parser.add_argument('--delta', type=float, default=5.0, help="Delta step")
    arg_parser.add_argument('--out', type=str, default=None, help="Out file")

    args = arg_parser.parse_args()
    if args.input == None:
        ValueError("No input data")
    delta = args.delta

    graph = read_graph(args.input)

    ## sync
    #if rank == 0:
    #    for i in range(1, nproc):
    #        comm.send(0, i, tag = 10)
    #else:
    #    comm.recv(source=0, tag = 10)
    
    if rank != 0:
        graph = update_graph_info(graph)
        comm.send(rank, dest=0, tag = 11)
    else:
        for i in range(1, nproc):
            comm.recv(source=i, tag = 11)


    graph = deltastepping(graph)
            
    #exchange()

    #print(f'End Rank {rank}')
    print(f'End Rank {rank}, graph: {graph}')