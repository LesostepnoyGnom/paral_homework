from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD # запускаем процессы
numprocs = comm.Get_size() # колличество процессов
rank = comm.Get_rank() # идентификаторы процессов

if rank == 0:
    message = b"Hello, world!"
else:
    message = np.array(0, dtype = np.int32)


if rank == 0:
    for k in range(1, numprocs):
        comm.Send([message, 1, MPI.CHAR], dest=k, tag=0)
else:
    buf = bytearray(256)
    comm.Recv([buf, 1, MPI.CHAR], source=0, tag=0, status=None)
    # print(buf.decode('utf-8'))

print('Message on process {0} is: {1}'.format(rank, message))