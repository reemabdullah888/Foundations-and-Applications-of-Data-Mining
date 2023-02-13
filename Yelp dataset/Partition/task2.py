import sys
from pyspark import SparkContext
import json
import os
from time import perf_counter

# collect the code into main() function to be run
def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    json_file = sys.argv[1]
    output_file = sys.argv[2]
    num_partition = sys.argv[3]
    result = dict()

    # create SparkContext object
    sc = SparkContext.getOrCreate()
    # load json as text
    json_d = sc.textFile(json_file)
    # convert json into RDD
    rdd_data = json_d.map(json.loads)

    # Create the output result
    result["default"]= default_func(rdd_data)
    result["customized"] = cusomize_func(rdd_data, num_partition)

    create_json(output_file,result)
def create_json(output_file, result):
    j_result = json.dumps(result) # transfer python dictionary into json
    with open(output_file, 'w+') as f:
        f.write(j_result)
    f.close()


def cusomize_func(rdd, num_partition):
    # Start recording the start of execution time
    time_start = perf_counter()
    business = rdd.map(lambda x: (x['business_id'], 1))
    hashed_business = business.partitionBy(int(num_partition), lambda key: hash(key[0]))
    sorted_business = hashed_business.map(lambda s: (s[0], s[1])).sortBy(lambda x: [x[0], x[1]])  # sort in descending order
    b2 = sorted_business.map(lambda s: [s[1], s[0]]).sortByKey(False).map(lambda s: ([s[1], s[0]])).take(10)
    time_stop = perf_counter()
    num_partition = hashed_business.getNumPartitions()   # A  calculate number of partition
    num_items = hashed_business.glom().map(len).collect()  # B count number of items per partition
    difference = time_stop - time_start   # C calculate the execution time


    # storing the result in a dictionary
    r2 = {}
    r2["n_partition"] = num_partition
    r2["n_items"] = num_items
    r2["exe_time"] = difference
    return r2
def default_func(rdd):

    # Start recording the start of execution time
    time_start = perf_counter()
    business = rdd.map(lambda x: (x['business_id'], 1))
    reduce_business = business.reduceByKey(lambda x, y: x + y).map(lambda s: (s[0], s[1])).sortBy(lambda x: [x[0], x[1]])  # sort in descending order
    b2 = reduce_business.map(lambda s: [s[1], s[0]]).sortByKey(False).map(lambda s: ([s[1], s[0]])).take(10)
    time_stop = perf_counter()
    num_partition = business.getNumPartitions()   # A
    num_items = business.glom().map(len).collect()  # B count number of items per partition
    difference = time_stop - time_start   # C calculate the execution time


    # storing the result in a dictionary
    r1 = {}
    r1["n_partition"] = num_partition
    r1["n_items"] = num_items
    r1["exe_time"] = difference
    return r1




if __name__ == '__main__':
    main()

