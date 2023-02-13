import sys
from pyspark import SparkContext

import json
import os

# collect the code into main() function to be run
def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    json_file = sys.argv[1]
    output_file = sys.argv[2]
    result = dict()


    # create SparkContext object
    sc = SparkContext.getOrCreate()
    # load json as text
    json_d = sc.textFile(json_file)
    # convert json into RDD
    rdd_data = json_d.map(json.loads)


    # Solving the questions
    result["n_review"] = num_reviews(rdd_data)  # A
    result["n_review_2018"] = num_rev_2018(rdd_data)  # B
    result["n_user"] = wrote_review(rdd_data)  # C
    result["top10_user"] = largest_review(rdd_data)  # D
    result["n_business"] = distinct_business(rdd_data)  # E
    result["top10_business"] = top_businesses(rdd_data)  # F
    #
    # print("\n \n \n")
    # print(top_businesses(rdd_data))
    #
    # print("\n \n \n")


    create_json(output_file,result)
def create_json(output_file, result):
    j_result = json.dumps(result)
    with open(output_file, 'w+') as f:
        f.write(j_result) # transfer python dictionary into json
    f.close()

def num_reviews(r):
    review = r.map(lambda x: x['review_id']).count() # only take review_id
    return review


def num_rev_2018(rdd):
    date = rdd.map(lambda x: x['date']) # Takes dates ONLY
    y_2018 = date.filter(lambda f: '2018' in f)
    return y_2018.count()

def wrote_review(rdd):
    user = rdd.map(lambda x: x["user_id"]).distinct()
    return user.count()

def largest_review(rdd):

    user = rdd.map(lambda x: (x["user_id"], 1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x: [x[0], x[1]])
    v_k = user.map(lambda x:(x[1],x[0])).sortByKey(False)
    last_edit = v_k.map(lambda x:([x[1],x[0]]))
    return last_edit.take(10)






def distinct_business(rdd):
    business = rdd.map(lambda x: x['business_id']).distinct().count()
    return business

def top_businesses(rdd):
    business = rdd.map(lambda x: (x['business_id'],1)).reduceByKey(lambda x,y:x+y).map(lambda s: (s[0],s[1])).sortBy(lambda x: [x[0], x[1]])# sort in descending order
    b2 = business.map(lambda s: [s[1],s[0]]).sortByKey(False).map(lambda s: ([s[1],s[0]]))
    return b2.take(10)

if __name__ == '__main__':
    main()
