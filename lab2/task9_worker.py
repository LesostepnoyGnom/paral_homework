# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 22:17:13 2023

@author: 1618047
"""

from mpi4py import MPI

comm = MPI.Comm.Get_parent()
rank = comm.Get_rank()

print("I'm a worker and my rank is", rank)
message = b"Hello, master!"
comm.Send([message, 14, MPI.CHAR], dest=0, tag=0)

comm.Disconnect()