import time
import pandas as pd
from tqdm import tqdm
from threading import Thread
from threading import Lock

num_of_threads = int(input("Enter Total threads: "))
number_of_rows = 6311872
mapping_output = []
path = '/Users/princeghoghari/Library/CloudStorage/OneDrive-ConcordiaUniversity-Canada/COMP6231/Dataset/Combined_Flights_2021.csv'

def map_tasks(reading_info: list, lock):
    global mapping_output
    dataframe = pd.read_csv(path, nrows=reading_info[1], skiprows=reading_info[0], header=None)
    filtered_data = dataframe.iloc[:, [0, 1, 5]]
    dataframe2 = filtered_data[(filtered_data.iloc[:, 0].between('2021-11-20', '2021-11-30')) & (filtered_data.iloc[:, 2] == True)]
    lock.acquire()
    mapping_output.append(dataframe2.iloc[:, 1].value_counts())
    lock.release()

def reduce_task(mapping_output):
    reduce_out = {}
    for out in tqdm(mapping_output):
        for key, value in out.to_dict().items():
            if key in reduce_out:
                reduce_out[key] = reduce_out.get(key) + value
            else:
                reduce_out[key] = value

    print(f"'{sum(reduce_out.values())}' flights were diverted in between 20 november 2021 and 30 november 2021.")

def Threading():
    global num_of_threads
    global number_of_rows
    thread_handle = []
    lock = Lock()

    for i in range(0, num_of_threads):
        t = Thread(target=map_tasks,
                   args=(
                       [[int((number_of_rows / num_of_threads) * i) + 1, int(number_of_rows / num_of_threads)], lock]))
        thread_handle.append(t)
        print([int(number_of_rows / num_of_threads), int((number_of_rows / num_of_threads) * i) + 1])
        t.start()

    for j in range(0, num_of_threads):
        thread_handle[j].join()

    reduce_task(mapping_output)

if __name__ == "__main__":
    start_time = time.time()
    Threading()
    end_time = time.time()
    print(
        f'time taken with {num_of_threads} threads: {round(time.time() - start_time, 2)} second(s)')



