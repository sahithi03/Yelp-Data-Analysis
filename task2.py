from pyspark import SparkContext
import sys
import json
import time

start = time.time()
sc = SparkContext('local[*]','task2')
sc.setLogLevel("ERROR")

review_file = sys.argv[1]
output_file = sys.argv[2]
custom_n_partitions = int(sys.argv[3])

data = sc.textFile(review_file)
business_rdd = data.map(json.loads).map(lambda col:(col['business_id'],col['business_id'])).persist()

# *************************default partitions*****************************

default_n_partitions = business_rdd.getNumPartitions()
default_list = business_rdd.glom().map(len).collect()

default_start_time = time.time()
top10_business_default = business_rdd.map(lambda x:(x[0],1)).reduceByKey(lambda a,b:a+b).takeOrdered(10,lambda x:(-x[1],x[0]))
default_end_time = time.time()
default_exec_time = default_end_time - default_start_time

default = {
        "n_partition":default_n_partitions,
        "n_items":default_list,
        "exe_time":default_exec_time
    }



# ************************custom partitions***************************************

def custom_partition(business_id):
    return hash(business_id)


custom_rdd = business_rdd.partitionBy(custom_n_partitions,custom_partition)
collect_start = time.time()
custom_list = custom_rdd.glom().map(len).collect()


custom_start_time = time.time()

top_business_reviews_2 = custom_rdd.map(lambda x:(x[0],1)).reduceByKey(lambda a,b:a+b).takeOrdered(10,lambda x:(-x[1],x[0]))

custom_end_time = time.time()
custom_exec_time = custom_end_time - custom_start_time

business_rdd.unpersist()

customized = {
    "n_partition":custom_n_partitions,
    "n_items":custom_list,
    "exe_time":custom_exec_time
}

results = {}
results["default"] = default
results["customized"] = customized

print("Task 2 Execution time",time.time()-start)

with open(output_file, 'w') as outfile:
    json.dump(results, outfile)