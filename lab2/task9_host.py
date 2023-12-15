# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 22:17:10 2023

@author: 1618047
"""

from mpi4py import MPI
import sys

comm = MPI.COMM_WORLD
numprocs = comm.Get_size()
rank = comm.Get_rank()

print("I'm a host and my rank is", rank)
print("I made",numprocs,"workers")
comm = comm.Spawn(sys.executable, args=["task9_worker.py"], maxprocs=numprocs)

for k in range(1, numprocs):
    buf = bytearray(64)
    comm.Recv([buf, 14, MPI.CHAR], source=k, tag=0, status=None)
    print("I received message from worker {0}: {1}".format(k, buf.decode('utf-8')))
    
comm.Disconnect()