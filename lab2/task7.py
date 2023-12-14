# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 13:44:06 2023

@author: 1618047
"""

from mpi4py import MPI
import numpy as np
import sys

comm = MPI.COMM_WORLD
numprocs = comm.Get_size()
rank = comm.Get_rank()

n = 15

if rank == 0:
    message = b"Hello, world!"
    comm.Send([message, n, MPI.CHAR], dest=1, tag=0)
    print('Message on process {0} is: {1}'.format(rank, message))


for i in range(1,numprocs):
    if rank == i:
        buf = bytearray(64)
        comm.Recv([buf, n, MPI.CHAR], source=i-1, tag=0, status=None)
        if i == numprocs-1:
            comm.Send([buf, n, MPI.CHAR], dest=0, tag=0)
        else:
            comm.Send([buf, n, MPI.CHAR], dest=i+1, tag=0)
        print('Message on process {0} is: {1}'.format(rank, buf.decode('utf-8')))

if rank == 0:
    buf_end = bytearray(64)
    comm.Recv([buf_end, n, MPI.CHAR], source=i, tag=0, status=None)
    print('Message on final process is: {0}'.format(buf_end.decode('utf-8')))
    print("Done")
    
# if rank == 1:
#     buf1 = bytearray(64)
#     comm.Recv([buf1, n, MPI.CHAR], source=0, tag=0, status=None)
#     comm.Send([buf1, n, MPI.CHAR], dest=2, tag=0)
#     # print(buf1.decode('utf-8'))
#     print('Message on process {0} is: {1}'.format(rank, buf1.decode('utf-8')))
    
# if rank == 2:
#     buf2 = bytearray(64)
#     comm.Recv([buf2, n, MPI.CHAR], source=1, tag=0, status=None)
#     comm.Send([buf2, n, MPI.CHAR], dest=3, tag=0)
#     # print(buf2.decode('utf-8'))
#     print('Message on process {0} is: {1}'.format(rank, buf2.decode('utf-8')))
    
# if rank == 3:
#     buf3 = bytearray(64)
#     comm.Recv([buf3, n, MPI.CHAR], source=2, tag=0, status=None)
#     comm.Send([buf3, n, MPI.CHAR], dest=4, tag=0)
#     print('Message on process {0} is: {1}'.format(rank, buf3.decode('utf-8')))

# if rank == 4:
#     buf4 = bytearray(64)
#     comm.Recv([buf4, n, MPI.CHAR], source=3, tag=0, status=None)
#     comm.Send([buf4, n, MPI.CHAR], dest=0, tag=0)
#     print('Message on process {0} is: {1}'.format(rank, buf4.decode('utf-8')))



# else:
#     buf = bytearray(256)
#     comm.Recv([buf, 1, MPI.CHAR], source=0, tag=0, status=None)
    
# if rank == 9:
#     print("DONE")

# print('Message on process {0} is: {1}'.format(rank, message))