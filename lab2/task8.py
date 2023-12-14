# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 22:32:00 2023

@author: 1618047
"""

from mpi4py import MPI
import numpy as np
import time

comm = MPI.COMM_WORLD
numprocs = comm.Get_size()
rank = comm.Get_rank()

n = 15

req = [MPI.Request() for i in range(2)]
signal = np.array(0, dtype = np.int32)

if rank == 0:
    message = b"Ya hochu pitsu!"
    # signal = np.array(0)
else:
    message = np.array(0, dtype = "<U13")

if rank == 0:
    # for k in range(1, numprocs):
    req[0] = comm.Isend([message, n, MPI.CHAR], dest=1, tag=0)
    
    req[1] = comm.Irecv([signal, 1, MPI.INT], source=1, tag=0)
    print("M: I'm waiting your signal")
    while MPI.Request.Test(req[1]) == False:
        print("WAITING")
        time.sleep(5)
        
    print("M: Got the signal. Thanks")
    print('signal =',signal)
if rank == 1:
    time.sleep(25)
    buf = bytearray(64)
    req[1] = comm.Irecv([buf, n, MPI.CHAR], source=0, tag=0)
    signal = np.array(1, dtype = np.int32)
    req[0] = comm.Isend([signal, 1, MPI.INT], dest=0, tag=0)