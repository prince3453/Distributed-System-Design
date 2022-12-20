from threading import Thread
from tqdm import tqdm
import time
import pandas as pd
from threading import Lock

total_threads = int(input("Enter total threads: "))
path = '/Users/princeghoghari/Library/CloudStorage/OneDrive-ConcordiaUniversity-Canada/COMP6231/Dataset/Combined_Flights_2021.csv'
map_output = []
number_of_rows = 6311872

def map_task(reading_info: list, lock):
    global map_output
    dataframe = pd.read_csv(path, nrows=reading_info[1], skiprows=reading_info[0], header=None)
    filtered_data = dataframe.iloc[:, [0, 1, 4]]
    dataframe2 = filtered_data[(filtered_data.iloc[:, 0].between('2021-09-01', '2021-09-31') & filtered_data.iloc[:, 2] == True)]
    lock.acquire()
    map_output.append(dataframe2.iloc[:, 1].value_counts())
    lock.release()

def reduce_task(map_output):
    reduce_out = {}
    for out in tqdm(map_output):
        for key, value in out.to_dict().items():
            if key in reduce_out:
                reduce_out[key] = reduce_out.get(key) + value
            else:
                reduce_out[key] = value

    most_cancelled_flight = sorted(reduce_out.items(), key=lambda x: x[1],reverse=True)
    print(f"'{most_cancelled_flight[0][0]}' airline had Maximum Cancelled flights in Sept 2021.")

def Threading():
    global total_threads
    global number_of_rows
    thread_handle = []
    lock = Lock()

    for i in range(0, total_threads):
        t = Thread(target=map_task,
                   args=(
                       [[int((number_of_rows / total_threads) * i) + 1, int(number_of_rows / total_threads)], lock]))
        thread_handle.append(t)
        print([int(number_of_rows / total_threads), int((number_of_rows / total_threads) * i) + 1])
        t.start()

    for j in range(0, total_threads):
        thread_handle[j].join()

    reduce_task(map_output)

if __name__ == "__main__":
    start_time = time.time()
    Threading()
    end_time = time.time()
    print(
        f'time taken with {total_threads} threads: {round(time.time() - start_time, 2)} second(s)')