from mpi4py import MPI
import argparse
import numpy as np
import json

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
nproc = comm.Get_size()

source = 0
delta = 0.01
d = {}
B = {}

#print(f'Rank: {rank}, nproc: {nproc}')

def read_graph(filename):
    if rank == 0:
        file = open(filename, "rb")
        if not file:
            ValueError("Can't open file")

        nodes = np.frombuffer(file.read(4), np.uint32)[0]
        arity = np.frombuffer(file.read(8), np.uint64)[0]
        directed = np.frombuffer(file.read(1), np.bool8)[0]
        assert(directed == False)
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
        rowIndicesonproc = nodes // (nproc - 1)
        rowIndicesoffset = 0
        nodesoffset = 0
        for i in range(1, nproc):
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

def prepare_graph(graph):
    graph["ginfo"] = {}
    graph["g2lnodes"] = {}

    for i in range(graph["nodesLocal"]):
        globalnode = i + graph["nodesOffset"]
        graph["g2lnodes"][int(globalnode)] = i
        if globalnode not in graph["ginfo"]:
            graph["ginfo"][globalnode] = {
                "nodes": [],
                "weights": []
            }
        for j in range(graph["rowIndicesLocal"][i], graph["rowIndicesLocal"][i+1]):
            localendVidx = j - graph["rowIndicesLocal"][0]
            endVnode = graph["endVLocal"][int(localendVidx)]
            edgeweight = graph["weightsLocal"][int(localendVidx)]
            graph["ginfo"][globalnode]["nodes"].append(endVnode)
            graph["ginfo"][globalnode]["weights"].append(edgeweight)
            if endVnode not in graph["ginfo"]:
                if (endVnode - graph["nodesOffset"] < 0) or (endVnode - graph["nodesOffset"] >= graph["nodesLocal"]):
                    graph["ginfo"][endVnode] = {
                        "nodes": [ globalnode ],
                        "weights": [ edgeweight ]
                    }
            else:
                if endVnode not in graph["g2lnodes"]:
                    if (endVnode - graph["nodesOffset"] < 0) or (endVnode - graph["nodesOffset"] >= graph["nodesLocal"]):
                        graph["ginfo"][endVnode]["nodes"].append(globalnode)
                        graph["ginfo"][endVnode]["weights"].append(edgeweight)

    return graph

def node2proc(node):
    all = graph["nodesAll"]
    count_on_proc = all // (nproc - 1)

    i_proc = node // count_on_proc

    return i_proc + 1

def bucketdict2proc(B, i):
    B_to_i = {}

    for key in B.keys():
        for node in B[key]:
            if node2proc(node) == i:
                if key not in B_to_i:
                    B_to_i[key] = [ node ]
                else:
                    B_to_i[key].append(node)
    return B_to_i

def d2proc(d, i):
    d_to_i = {}

    for key in d.keys():
        if node2proc(key) == i and d[key] != float("inf"):
            d_to_i[int(key)] = d[key]

    return d_to_i

def exchange():
    # first sync
    if rank == 0:
        for i in range(1, nproc):
            comm.send((f'sync1 {i}'), dest=i, tag = 1)
    else:
        comm.recv(source=0, tag = 1)

    B_from_proc = {}
    d_from_proc = {}

    if rank != 0:
        for i in range(1, nproc):
            if i != rank:
                B_to_i = bucketdict2proc(B, i)
                d_to_i = d2proc(d, i)
                #print(f'{i}: d_to_i: {len(d_to_i.keys())}')
                #print(f'{i}: B_to_i: {len(B_to_i.keys())}')
                comm.isend(B_to_i, dest=i, tag = (2+i))
                comm.isend(d_to_i, dest=i, tag = (20+i))
                #comm.isend(d, dest=i, tag = 20)
    
        for i in range(1, nproc):
            if i != rank:
                B_from_proc[i] = comm.recv(source=i, tag = (2+rank))
                d_from_proc[i] = comm.recv(source=i, tag = (20+rank))

    ## second sync
    #if rank == 0:
    #    for i in range(1, nproc):
    #        comm.send(i, dest=i, tag = 4)
    #else:
    #    comm.recv(source=0, tag = 4)

    if rank == 0:
        for i in range(1, nproc):
            comm.send(i, dest=i, tag = 8)
    if rank != 0:
        for i in range(1, nproc):
            if i != rank:
                for key in (B_from_proc[i].keys()):
                    if B_from_proc[i][key]:
                        if key not in B:
                            B[key] = B_from_proc[i][key]
                        else:
                            for val in B_from_proc[i][key]:
                                if val not in B[key]:
                                    B[key].append(val)
                for node in d_from_proc[i].keys():
                    if node not in d:
                        d[node] = d_from_proc[i][node]
                    else:
                        if d_from_proc[i][node] < d[node]:
                            d[node] = d_from_proc[i][node]
        comm.recv(source=0, tag = 8)

    # sync B existance
    existB = False
    if rank != 0:
        if B:
            existB = True
        for i in range(1, nproc):
            if i != rank:
                comm.isend(existB, dest=i, tag = 9)

        for i in range(1, nproc):
            if i != rank:
                existB_i = comm.recv(source=i, tag = 9)
                if existB_i != existB:
                    existB = True

    if rank == 0:
        existB = comm.recv(source=1, tag = 10)
    else:
        if rank == 1:
            comm.send(existB, dest=0, tag = 10)

    return existB

def relax(reqs):
    for node in reqs:
        OldBucket = []
        NewBucket = []
        #print(f'{rank}: reqs: {reqs[node]}')
        #print(f'{rank}: d: {d[node]}')
        if node not in d:
            d[int(node)] = float("inf")
        if reqs[node] < d[node]:
            # change B
            x = int(reqs[node])
            if float(d[node]/delta) in B:
                OldBucket = B[float(d[node]/delta)]
            if float(x/delta) in B:
                NewBucket = B[float(x/delta)]
            if node in OldBucket:
                OldBucket.remove(node)
                B[float(d[node]/delta)] = OldBucket
            if node not in NewBucket:
                NewBucket.append(int(node))
                B[float(x/delta)] = NewBucket
            d[int(node)] = reqs[int(node)]

    return

def findrequests(graph, bucket):
    reqs = {}
    for node in bucket:
        if node in graph["ginfo"]:
            for j in range(len(graph["ginfo"][node]["nodes"])):
                if graph["ginfo"][node]["nodes"][j] in reqs:
                    if reqs[graph["ginfo"][node]["nodes"][j]] > d[node] + graph["ginfo"][node]["weights"][j]: 
                        reqs[graph["ginfo"][node]["nodes"][j]] = d[node] + graph["ginfo"][node]["weights"][j]
                else:
                    reqs[graph["ginfo"][node]["nodes"][j]] = d[node] + graph["ginfo"][node]["weights"][j]
    return reqs

def deltastepping(graph):
    if rank != 0:
        for i in range(len(graph["ginfo"].keys())):
            d[int(i)] = float("inf")

    if rank != 0:
        if source in graph["ginfo"]:
            d[int(source)] = 0
            B[0.0] = [ source ]

    cycle = 0
    existB = True
    while existB:
        if B:
            i = min(B.keys())
            reqs = findrequests(graph, B[i])
            del B[i]
            relax(reqs)
        existB = exchange()
        cycle += 1

    return graph

def write_to_out(filename):
    # TODO Maybe i do not need this sync
    if rank == 0:
        for i in range(1, nproc):
            comm.send((f'sync1 {i}'), dest=i, tag = 80)
    else:
        comm.recv(source=0, tag = 80)

    #print(f'{rank}: {d}')

    if rank == 0:
        d_from_i = {}
        for i in range(1, nproc):
            d_from_i[i] = comm.recv(source=i, tag=81)

        d_total = {}
        for i in range(1, nproc):
            for key in d_from_i[i].keys():
                if key not in d_total:
                    #print(d_from_i[i][key])
                    d_total[key] = d_from_i[i][key]
                else:
                    if d_from_i[i][key] < d_total[key]:
                        d_total[key] = d_from_i[i][key]
        if filename != None:
            f = open(filename, "w")
            json.dump(d_total, f)
            #for key in d.keys():
            #    f.write(f'{key}: {d_total[key]}\n')
            f.close()
        else:
            print(d_total)
    else:
        comm.send(d, dest=0, tag=81)

    return

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Parallel DeltaStepping algorithm")
    arg_parser.add_argument('--input', type=str, default=None, help="Binary file with graph information")
    arg_parser.add_argument('--delta', type=float, default=5.0, help="Delta step")
    arg_parser.add_argument('--out', type=str, default=None, help="Out file")
    arg_parser.add_argument('--source', type=int, default=0, help="Source vertice")

    args = arg_parser.parse_args()
    if args.input == None:
        ValueError("No input data")
    delta = args.delta
    source = args.source

    graph = read_graph(args.input)

    if rank != 0:
        graph = prepare_graph(graph)
        comm.send(rank, dest=0, tag = 11)
    else:
        for i in range(1, nproc):
            comm.recv(source=i, tag = 11)

    start_time = MPI.Wtime()
    graph = deltastepping(graph)
    ts_duration = MPI.Wtime() - start_time
    if rank == 0:
        print(f'ts: {ts_duration}')

    # sync
    write_to_out(args.out)