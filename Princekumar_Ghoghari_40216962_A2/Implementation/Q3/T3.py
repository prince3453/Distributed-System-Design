import pandas as pd
from mpi4py import MPI
from tqdm import tqdm
import time

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
number_of_rows = 6311872

dataset = '/Users/princeghoghari/Library/CloudStorage/OneDrive-ConcordiaUniversity-Canada/COMP6231/Dataset/Combined_Flights_2021.csv'

if rank == 0:
    """
    Master worker (with rank 0) is responsible for distributes the workload evenly 
    between slave workers.
    """
    def distribute_rows(n_rows: int, n_processes):
        reading_info = []
        skip_rows = 1
        reading_info.append([n_rows - skip_rows, skip_rows])
        skip_rows = n_rows

        for _ in range(1, n_processes - 1):
            reading_info.append([n_rows, skip_rows])
            skip_rows = skip_rows + n_rows

        reading_info.append([None, skip_rows])
        return reading_info


    slave_workers = size - 1
    start = time.time()
    nrows = number_of_rows // slave_workers
    chunk_distribution = distribute_rows(n_rows=nrows, n_processes=slave_workers)

    # distribute tasks to slaves
    for worker in range(1, size):
        chunk_to_process = worker - 1
        comm.send(chunk_distribution[chunk_to_process], dest=worker)

    # receive and aggregate results from slave
    results = []
    for worker in (range(1, size)):  # receive
        result = comm.recv(source=worker)
        results.append(result)
        print(f'received from Worker slave {worker}')

    reduce_out = {}
    for out in tqdm(results):
        for key, value in out.to_dict().items():
            if key in reduce_out:
                reduce_out[key] = reduce_out.get(key) + value
            else:
                reduce_out[key] = value
    sum_of_time =0
    for i, j in reduce_out.items():
        sum_of_time = sum_of_time + (i * j)
    total_value = sum(reduce_out.values())
    avg_time = sum_of_time / total_value
    print(f'Average flight time from nashville to chicago : {avg_time}')

    end = time.time()
    print(f'Time taken for doing task with {size} workers : {end - start}', '\ndone.')


elif rank > 0:
    chunk_to_process = comm.recv()
    print(f'Worker {rank} is assigned chunk info {chunk_to_process} {dataset}')
    dataframe = pd.read_csv(dataset, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], header=None)
    filtered_data = dataframe.iloc[:, [2, 3, 12]]
    dataframe2 = filtered_data[(filtered_data.iloc[:, 0] == 'BNA') & (filtered_data.iloc[:, 1] == 'ORD')]
    result = dataframe2.iloc[:, 2].value_counts()
    print(f'Worker slave {rank} is done. Sending back to master')
    comm.send(result, dest=0)
