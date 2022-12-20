import multiprocessing
import time
import pandas as pd
from multiprocessing import Pool
from tqdm import tqdm as tqdm

number_of_rows = 6311872

def map_tasks(reading_info: list, data: str = '/Users/princeghoghari/Library/CloudStorage/OneDrive-ConcordiaUniversity-Canada/COMP6231/Dataset/Combined_Flights_2021.csv'):
    dataframe = pd.read_csv(data, nrows=reading_info[0], skiprows=reading_info[1], header=None)
    filtered_data = dataframe.iloc[:,[0,1,5]]
    dataframe2 = filtered_data[(filtered_data.iloc[:,0].between('2021-11-20','2021-11-30')) & (filtered_data.iloc[:,2] == True)]
    return dataframe2.iloc[:, 1].value_counts()

def reduce_task(mapping_output):
    reduce_out ={}
    for out in tqdm(mapping_output):
        for key, value in out.to_dict().items():
            if key in reduce_out:
                reduce_out[key] = reduce_out.get(key) + value
            else:
                reduce_out[key] = value

    print(f"'{sum(reduce_out.values())}' flights were diverted in between 20 november 2021 and 30 november 2021.")

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