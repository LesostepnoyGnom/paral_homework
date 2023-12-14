from mpi4py import MPI
import time

comm = MPI.COMM_WORLD
rank = MPI.COMM_WORLD.Get_rank()

def sleep_for(t=10):
    time.sleep(t)

if rank == 0:
    sleep_for(5)
    req = comm.irecv(source=1, tag=0)
    msg = req.wait()
    
    print("Hello, world!")
    print("time = ", round((time.time() - msg)*1000, 3),'ms.')

if rank == 1:
    req = comm.isend(time.time(), dest=0, tag=0)
    req.wait()
    for i in range(5):
        print('*')
        sleep_for(2)
