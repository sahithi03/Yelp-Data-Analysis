from pyspark import SparkContext
import sys
import json
import time

start = time.time()
sc = SparkContext('local[*]', 'task3')
sc.setLogLevel("ERROR")

review_file = sys.argv[1]
business_file = sys.argv[2]
output_file_a = sys.argv[3]
output_file_b = sys.argv[4]

review_data = sc.textFile(review_file)
business_data = sc.textFile(business_file)

review_rdd = review_data.map(json.loads).map(lambda col: (col['business_id'], col['stars'])).repartition(16)

business_rdd = business_data.map(json.loads).map(lambda col: (col['business_id'], col['city']))

# *************** PART A *************************

aggregate = business_rdd.join(review_rdd).map(lambda a: a[1]) \
    .mapValues(lambda a: (a, 1)) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

average_stars = aggregate.mapValues(lambda a: (a[0] / a[1]))

# *************** PART B ***************************

# ************* METHOD A ***************************
methodA_start = time.time()

python_list = average_stars.collect()

temp_list = []
for city in python_list:
    temp_list.append([city[0], city[1]])
temp_list.sort(key=lambda x: (-x[1], x[0]))

print("METHOD 1\n")
print("city,stars\n")

for i in range(10):
    print(temp_list[i][0] + "," + str(temp_list[i][1]))

methodA_end = time.time()
methodA_exec = methodA_end - methodA_start

# ************* METHOD B *******************************

methodB_start = time.time()

spark_rdd = average_stars.sortByKey(ascending=True).map(lambda a: (a[1], a[0])).sortByKey(ascending=False).map(
    lambda a: (a[1], a[0]))
spark_list = spark_rdd.take(10)
all_results = spark_rdd.collect()

print("\nMETHOD 2\n")
print("city,stars\n")
for city in range(10):
    print(spark_list[city][0] + "," + str(spark_list[city][1]))

methodB_end = time.time()
methodB_exec = methodB_end - methodB_start

with open(output_file_a, "w", encoding="utf-8") as outfile:
    outfile.write("city,stars\n")
    for result in all_results:
        outfile.write(str(result[0]) + "," + str(result[1]) + "\n")

execution_times = {}
execution_times["m1"] = methodA_exec
execution_times["m2"] = methodB_exec

with open(output_file_b, "w") as outfile2:
    json.dump(execution_times, outfile2)

print("Task 3 execution time", time.time() - start)