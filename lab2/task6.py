# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 12:54:26 2023

@author: 1618047
"""

from mpi4py import MPI
import numpy as np
import random
import time
import sys

def mk_v(l):
    v = []
    for i in range(l):
        v.append(random.randint(0, 10))
    return np.array(v)

comm = MPI.COMM_WORLD
numprocs = comm.Get_size()
rank = comm.Get_rank()

l = 50000
size = 1
N = 10

if rank == 0:
    while size <= l+1:
        v = mk_v(size)
        
        start_time = time.time()
        
        for i in range(N):
            comm.send(v, dest=1)
            req = comm.recv(source=1)
                
        finish_time = time.time()
            
        R = (2 * sys.getsizeof(v) * N) / (finish_time - start_time)
        print("length =", size, ", bytesize =", sys.getsizeof(v), ", bandwidth =", round(R/1024/1024, 3), "MB/s")
        size += 1000

else:
    while size <= l+1:
        for i in range(N):
            req = comm.recv(source=0)
            comm.send(req, dest=0)
        size += 1000