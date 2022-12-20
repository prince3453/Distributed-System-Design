import multiprocessing
import time
import pandas as pd
from multiprocessing import Pool
from tqdm import tqdm as tqdm

number_of_rows = 6311872

def map_tasks(reading_info: list, data: str = '/Users/princeghoghari/Library/CloudStorage/OneDrive-ConcordiaUniversity-Canada/COMP6231/Dataset/Combined_Flights_2021.csv'):
    dataframe = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    filtered_data = dataframe.iloc[:,[2,3,12]]
    dataframe2 = filtered_data[(filtered_data.iloc[:,0] == 'BNA') & (filtered_data.iloc[:,1] == 'ORD')]
    return dataframe2.iloc[:,2].value_counts()

def reduce_task(mapping_output: list):
    reduce_out = {}
    for out in tqdm(mapping_output):
        for key, value in out.to_dict().items():
            if key in reduce_out:
                reduce_out[key] = reduce_out.get(key) + value
            else:
                reduce_out[key] = value

    for i, j in reduce_out.items():
        sum_of_time = sum_of_time + (i * j)
    total_value = sum(reduce_out.values())
    avg_time = sum_of_time/total_value
    print(f'Average flight time from nashville to chicago : {avg_time}')

def compute_multiprocessing(n_processes: int = multiprocessing.cpu_count()):
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

    print('using multiprocessing')
    start = time.time()
    processes = multiprocessing.cpu_count()
    nrows = number_of_rows//processes
    p = Pool(processes=processes)
    result = p.map(map_tasks, distribute_rows(n_rows=nrows, n_processes=processes))
    reduce_task(result)
    p.close()
    finish = time.time()
    print(f'time taken with {n_processes} processes: {round(finish - start, 2)} second(s)')


if __name__ == '__main__':
    compute_multiprocessing()