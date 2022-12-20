import os
from itertools import permutations, count
from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
import pyspark.sql.functions as f

def restaurant_shift_coworkers(worker_shifts: RDD) -> RDD:
    """
    Takes an RDD that represents the contents of the worker_shifts.txt. Performs a series of MapReduce operations via
    PySpark to calculate the number of shifts worked together by each pair of co-workers. Returns the results as an RDD
    sorted by the number of shifts worked together THEN by the names of co-workers in a DESCENDING order.
    :param worker_shifts: RDD object of the contents of worker_shifts.txt.
    :return: RDD of pairs of co-workers and the number of shifts they worked together sorted in a DESCENDING order by
             the number of shifts then by the names of co-workers.
             Example output: [(('Shreya Chmela', 'Fabian Henderson'), 3),
                              (('Fabian Henderson', 'Shreya Chmela'), 3),
                              (('Shreya Chmela', 'Leila Jager'), 2),
                              (('Leila Jager', 'Shreya Chmela'), 2)]
    """
    # Split a line using , and store it into a RDD as [[str, str]]
    SplitData = worker_shifts.map(lambda items: items.split(','))
    # put name in list so we can combine it by date next [([str], str)]
    ElementName = SplitData.map(lambda items: ([items[0]], items[1]))
    #reverse the order [(str,[str])]
    ReducedBKey = ElementName.map(lambda items : (items[1], items[0]))
    # reduceed it by the date [(str,[str,str,str,..])]
    reduced = ReducedBKey.reduceByKey(lambda value1, value2: value1+value2)
    # store first element to permute it with possible name according to dates
    ElementFirst = reduced.map(lambda items : items[1])
    # Several possible Variation of two names from RDD [(str, str), 1)]
    PairingPermuted = ElementFirst.map(lambda items: tuple([i, 1] for i in list(permutations(items, 2))))
    # Assign every possible permutation to  1 [((str, str), 1)]
    CountOfPairs = PairingPermuted.flatMap(lambda items: [i for i in items])
    # first it reduce key and count the total and then it sort in decendinf order [((str, str), total)]
    pairs = CountOfPairs.reduceByKey(lambda value1, value2: value1+value2).sortBy(lambda items: (items[1], items[0]), ascending=False)
    return pairs

def air_flights_most_canceled_flights(flights: DataFrame) -> str:
    """
    Takes the flight data as a DataFrame and finds the airline that had the most canceled flights on Sep. 2021
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The name of the airline with most canceled flights on Sep. 2021.
    """
    Filtered_Cancelled = flights.filter(flights["Cancelled"] == True)
    Cancel = Filtered_Cancelled.filter(f.col('FlightDate').between('2021-09-01','2021-09-30'))
    Cancelled_Flights = Cancel.select(Filtered_Cancelled['Airline'])
    FilteredSortedCancelledData = Cancelled_Flights.groupBy('Airline').count().orderBy('count', ascending=False)
    FirstElementOfRow = FilteredSortedCancelledData.collect()
    return FirstElementOfRow[0].__getitem__('Airline')

def air_flights_diverted_flights(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and calculates the number of flights that were diverted in the period of 
    20-30 Nov. 2021.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The number of diverted flights between 20-30 Nov. 2021.
    """
    Filtered_diverted = flights.filter(flights["Diverted"] == True)
    divert = Filtered_diverted.filter(f.col('FlightDate').between('2021-11-20', '2021-11-30'))
    diverted_flights = divert.select(Filtered_diverted['Airline']).count()
    return diverted_flights

def air_flights_avg_airtime(flights: DataFrame) -> float:
    """
    Takes the flight data as a DataFrame and calculates the average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: The average airtime average airtime of the flights from Nashville, TN to 
    Chicago, IL.
    """
    FilteredWithCity = flights.filter((flights['OriginCityName'] == 'Nashville, TN') & (flights['DestCityName'] == 'Chicago, IL'))
    FilteredWithValues = FilteredWithCity.select('AirTime').dropna()
    AverageTime = FilteredWithValues.select(f.avg('AirTime')).collect()
    return AverageTime[0].__getitem__(0)

def air_flights_missing_departure_time(flights: DataFrame) -> int:
    """
    Takes the flight data as a DataFrame and find the number of unique dates where the departure time (DepTime) is 
    missing.
    :param flights: Spark DataFrame of the flights CSV file.
    :return: the number of unique dates where DepTime is missing. 
    """
    FilteredUniqueDates = flights.filter(flights['DepTime'].isNull())
    UniqueDates = FilteredUniqueDates.select('FlightDate').distinct().count()
    return UniqueDates

def main():
    # initialize SparkContext and SparkSession
    sc = SparkContext('local[*]')
    spark = SparkSession.builder.getOrCreate()

    print('########################## Problem 1 ########################')
    # problem 1: restaurant shift coworkers with Spark and MapReduce
    # read the file
    worker_shifts = sc.textFile('worker_shifts.txt')
    sorted_num_coworking_shifts = restaurant_shift_coworkers(worker_shifts)
    # print the most, least, and average number of shifts together
    sorted_num_coworking_shifts.persist()
    print('Co-Workers with most shifts together:', sorted_num_coworking_shifts.first())
    print('Co-Workers with least shifts together:', sorted_num_coworking_shifts.sortBy(lambda x: (x[1], x[0])).first())
    print('Avg. No. of Shared Shifts:',
          sorted_num_coworking_shifts.map(lambda x: x[1]).reduce(lambda x,y: x+y)/sorted_num_coworking_shifts.count())

    print('########################## Problem 2 ########################')
    # problem 2: PySpark DataFrame operations
    # read the file
    flights = spark.read.csv('Combined_Flights_2021.csv', header=True, inferSchema=True)
    print('Q1:', air_flights_most_canceled_flights(flights), 'had the most canceled flights in September 2021.')
    print('Q2:', air_flights_diverted_flights(flights), 'flights were diverted between the period of 20th-30th '
                                                       'November 2021.')
    print('Q3:', air_flights_avg_airtime(flights), 'is the average airtime for flights that were flying from '
                                                   'Nashville to Chicago.')
    print('Q4:', air_flights_missing_departure_time(flights), 'unique dates where departure time (DepTime) was '
                                                              'not recorded.')
    

if __name__ == '__main__':
    main()
