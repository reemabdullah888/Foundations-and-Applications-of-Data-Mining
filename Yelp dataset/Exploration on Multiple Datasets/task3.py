import sys
from pyspark import SparkContext
import json
import os
from time import perf_counter


# collect the code into main() function to be run
def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    json_file1 = sys.argv[1]   # user data
    json_file2 = sys.argv[2]  # business data
    text_file = sys.argv[3]   # store output of A
    json_output = sys.argv[4]

    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    result = dict()

    json_data_users = sc.textFile(json_file1)
    json_data_business = sc.textFile(json_file2)
    # convert json into RDD
    rdd_data_user = json_data_users.map(json.loads)
    rdd_data_business = json_data_business.map(json.loads)
    # Join the two files [business data and user data]
    # Call
    join_average_sorted_final_result = join_user_business(rdd_data_user, rdd_data_business)  # A


    # Call
    # create the output file
    create_text(text_file, join_average_sorted_final_result)
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ Done A


    # Call
    spark_result = spark_exc_time(json_file1,json_file2)     # B1
    python_result = python_exc_time(json_file1,json_file2)  # B2
    # preparing the output
    result["m1"]= spark_result
    result["m2"]=python_result
    result["reason"]= "In my case the execution time for Python is faster than the execution time for Spark. Because both method uses the exact steps for loading and collecting the average except sorting. In python the list that has the information of cities and starts is stored in main memory, hence the sorting in Python is much faster. In Spark , I had operation to sortByKey which requires shuffling the data and we all know that shuffling isa  time consuming process since it redistributes the data so it can group keys across different partitions. As a result, Spark is less efficient in terms of execution time"
    # create json for output of B
    with open(json_output, 'w+') as ouput_file:
        json.dump(result, ouput_file)  # transfer python dictionary into json
    ouput_file.close()

# 1: join two datasets
def join_user_business(user, business):
    rdd_user = user.map(lambda item:(item["business_id"], item["stars"]))  # ('ujmEBvifdJM6h6RLv4wQIg', 1.0)
    rdd_business = business.map(lambda item:(item["business_id"], item["city"])) # [('1SWheh84yJXfytovILXOAQ', 'Phoenix')]
    # ('business_id',('city','star'))
    joined_user_business = rdd_business.join(rdd_user)   # ('AakkkTuGZA2KBodKi2_u8A', ('Toronto', 1.0))

    # call
    # call function to claculate the average value of a star
    join_average_sorted_final_result= average_stars(joined_user_business)
    return join_average_sorted_final_result




def average_stars(joined_user_business):
    city_star_without_id = joined_user_business.map(lambda x: x[1])  # [('Toronto', 1.0), ('Calgary', 5.0)]
    aggregated_val = city_star_without_id.aggregateByKey((0.0, 0), \
                    lambda initial, value: (initial[0] + value, initial[1] + 1), \
                    lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1]))  # [('City_name', (sum,count)), ('Gilbert', (5.0, 1))]
    # print("\n \n ", aggregated_val.take(2))
    avg_star = aggregated_val.mapValues(lambda v : v[0]/v[1])  # [('city_name', avg) ,
    # call
    # call function to store the average in desc order
    avg_star_Sorted = sort_stars(avg_star)     # correct_format_sorted.collect()
    return avg_star_Sorted


def sort_stars(avg_star):
# sort avgrage values in desc order
    sort_city_alpha = avg_star.sortBy(lambda x: (x[0], x[1]))  # first sort by city in alphabatical order * * second choose** ( ordered_city, avg)
    sort_avg_desc = sort_city_alpha.map(lambda item: (item[1], item[0])).sortByKey(False) # change the ouput format : (sorted_avg, ordered_city)
    correct_format_sorted = sort_avg_desc.map(lambda x: ([x[1], x[0]]))   # Put the right format back,  (ordered_city, sorted_avg)
    return correct_format_sorted.collect()


def create_text(text_file, result):
    with open(text_file, "w") as file:
        file.write("city,stars\n")
        for tuple in result:
            file.write(str(tuple[0]) + ',' + str(tuple[1]) + '\n')
    file.close()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ End A
def spark_exc_time(json_file1, json_file2): # Repeated stuff
    start_time = perf_counter()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    # loading data
    json_data_users = sc.textFile(json_file1)
    json_data_business = sc.textFile(json_file2)
    rdd_data_user = json_data_users.map(json.loads)
    rdd_data_business = json_data_business.map(json.loads)

    # join
    rdd_user = rdd_data_user.map(lambda item: (item["business_id"], item["stars"]))
    rdd_business = rdd_data_business.map(
        lambda item: (item["business_id"], item["city"]))
    joined_user_business = rdd_business.join(rdd_user)

    # calculate average
    city_star_without_id = joined_user_business.map(lambda x: x[1])
    aggregated_val = city_star_without_id.aggregateByKey((0.0, 0),lambda initial, value: (initial[0] + value, initial[1] + 1),lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1]))
    avg_star = aggregated_val.mapValues(lambda v: v[0] / v[1])

    # sort
    sort_city_alpha = avg_star.sortBy(lambda x: (x[0], x[1]))
    sort_avg_desc = sort_city_alpha.map(lambda item: (item[1], item[0])).sortByKey(False)
    correct_format_sorted = sort_avg_desc.map(lambda x: ([x[1], x[0]]))
    print(correct_format_sorted.take(10))


    end_time = perf_counter()
    difference = end_time - start_time
    return difference
def python_exc_time(json_file1,json_file2):
    start_time = perf_counter()
    sc = SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    # loading data
    json_data_users = sc.textFile(json_file1)
    json_data_business = sc.textFile(json_file2)
    rdd_data_user = json_data_users.map(json.loads)
    rdd_data_business = json_data_business.map(json.loads)

    # join
    rdd_user = rdd_data_user.map(lambda item: (item["business_id"], item["stars"]))
    rdd_business = rdd_data_business.map(
        lambda item: (item["business_id"], item["city"]))
    joined_user_business = rdd_business.join(rdd_user)

    # calculate average
    city_star_without_id = joined_user_business.map(lambda x: x[1])
    aggregated_val = city_star_without_id.aggregateByKey((0.0, 0),
                                                         lambda initial, value: (initial[0] + value, initial[1] + 1),
                                                         lambda rdd1, rdd2: (rdd1[0] + rdd2[0], rdd1[1] + rdd2[1]))
    avg_star = aggregated_val.mapValues(lambda v: v[0] / v[1])

    # sort using Python
    avg_star_list = avg_star.collect()
    sorted_avg_list= sorted(avg_star_list, key=lambda a: (-a[1],a[0]), reverse= False)  # ('city', avg), ('Chandler', 5.0)
    print(sorted_avg_list[:10])

    # end execution time
    end_time = perf_counter()
    difference = end_time - start_time
    return difference

if __name__ == '__main__':

    main()

