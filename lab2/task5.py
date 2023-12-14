# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 21:05:16 2023

@author: 1618047
"""

from mpi4py import MPI
import numpy as np
import random

def mk_v(l):
    v = []
    for i in range(l):
        v.append(round(random.random(), 3))
    return np.array(v)

comm = MPI.COMM_WORLD
numprocs = comm.Get_size()
rank = comm.Get_rank()

l = 1000000
if rank == 0:
    
    v1 = mk_v(l)
    v2 = mk_v(l)
    # print("vector1 = ", v1)
    # print("vector2 = ", v2)
    
else:
    v1 = np.empty(l, dtype=np.float64)
    v2 = np.empty(l, dtype = np.float64)
    
comm.Bcast([v1, l, MPI.DOUBLE], root=0) # отправляем вектора на все потоки
comm.Bcast([v2, l, MPI.DOUBLE], root=0)

if rank == 0:
    for k in range(1, numprocs):
        v1_part = np.empty(l//(numprocs-1), dtype=np.float64) # делаем пустышку для кусков вектора каждого работяги
        v2_part = np.empty(l//(numprocs-1), dtype=np.float64)
        
        for j in range(l//(numprocs-1)):
            v1_part[j] = np.float64(v1[j+(k-1)*2]) # делим исходные вектора на куски для работяг
            v2_part[j] = np.float64(v2[j+(k-1)*2])
            
        comm.Send([v1_part, l//(numprocs-1), MPI.DOUBLE], dest=k, tag=0) # отправляем куски вектора работягам
        comm.Send([v2_part, l//(numprocs-1), MPI.DOUBLE], dest=k, tag=1)
else:
    v1_part = np.empty(l//(numprocs-1), dtype=np.float64)
    v2_part = np.empty(l//(numprocs-1), dtype=np.float64)
    
    comm.Recv([v1_part, l//(numprocs-1), MPI.DOUBLE], source=0, tag=0, status=None)
    comm.Recv([v2_part, l//(numprocs-1), MPI.DOUBLE], source=0, tag=1, status=None)
    
res_part = np.empty(l//(numprocs-1), dtype=np.float64) # пустышка для кусков произведения
if rank != 0:
    res_part = np.dot(v1_part, v2_part) # считаем произведение на кусках
    # print("rank =", rank, "res_part =", res_part)
    
if rank == 0:
    res = np.empty(l, dtype=np.float64) # пустышка для конечного ответа
    
    for k in range(1, numprocs):
        comm.Recv([res_part , l//(numprocs-1), MPI.DOUBLE], source=k, tag=2, status=None) # принимаем расчитанные куски произведений от работяг
        for j in range(l//(numprocs-1)):
            res[(k-1)*l//(numprocs-1) + j] = res_part[j] # формируем вектор произведений
            
else: # rank != 0
    comm.Send([res_part , l//(numprocs-1), MPI.DOUBLE], dest=0, tag=2) # отправляем куски произведений
    
if rank == 0:
    # print("sum =", sum(res))
    print(res) # При больших векторах вместе суммы выводит NaN,
               # так что приходится выводить весь вектор скалярных произведений