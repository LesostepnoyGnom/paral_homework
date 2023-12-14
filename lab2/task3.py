from mpi4py import MPI
import numpy as np
import time

comm = MPI.COMM_WORLD
numprocs = comm.Get_size() 
rank = comm.Get_rank() 

class my_class:
    def my_fun(self):
        return 42

object1 = [3, 2, 2]
object2 = my_class().my_fun()
object3 = np.array([1,5,3,6], dtype=np.int32)

list_of_objects = [object1, object2, object3]

start_time = time.time()

if rank == 0:
    for k in range(1, numprocs):
        for i in range(len(list_of_objects)):
            comm.Send([np.array(list_of_objects[i]), MPI.INT32_T], dest=k, tag=i)
else:
    for i in range(len(list_of_objects)):
        comm.Recv([np.array(list_of_objects[i]), MPI.INT32_T], source=0, tag=i, status=None)

end_time = time.time()
d_time = end_time - start_time

for i in range(len(list_of_objects)):    
    print('Object{0} on process {1} is: {2}'.format(i, rank, list_of_objects[i]))
print('time = ', round(d_time*1000, 4), ' ms.')
print('')