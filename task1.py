from pyspark import SparkContext
import sys
import json
import time


sc = SparkContext('local[*]','task1')
sc.setLogLevel("ERROR")

start = time.time()
review_file = sys.argv[1]
output_file = sys.argv[2]

data = sc.textFile(review_file)

review_RDD = data.map(json.loads).map(lambda row:(row['user_id'],row['business_id'],row['date'])).repartition(16).persist()


#  ***** PART A ************
total_no_of_reviews = review_RDD.count()

# ****** PART B ************
no_of_reviews_2018 = review_RDD.map(lambda row:(row[2])).filter(lambda x:x[:4] == "2018").count()

# ******* PART C **********

no_of_distinct_users = review_RDD.map(lambda row:(row[0])).distinct().count()
#no_of_distinct_users = review_RDD.map(lambda row:(row[0])).distinct().count()

# ******* PART D ***********
top_users = review_RDD.map(lambda row:(row[0],1)).reduceByKey(lambda a,b:a+b).takeOrdered(10,lambda x : (-x[1],x[0]))
#top_users = review_RDD.map(lambda row:(row[0],1)).reduceByKey(lambda a,b:a+b).sortByKey(ascending=True).takeOrdered(10,lambda x:-x[1])

# ******* PART E ***********
no_of_distinct_businesses = review_RDD.map(lambda row:(row[1])).distinct().count()

# ******** PART F ************
top_business_reviews = review_RDD.map(lambda row:(row[1],1)).reduceByKey(lambda a,b:a+b).takeOrdered(10,lambda x:(-x[1],x[0]))

review_RDD.unpersist()

res = {}
res["n_review"]=total_no_of_reviews
res["n_review_2018"]=no_of_reviews_2018
res["n_user"]=no_of_distinct_users
res["top10_user"]=top_users
res["n_business"]=no_of_distinct_businesses
res["top10_business"]=top_business_reviews

with open(output_file, 'w') as outfile:
    json.dump(res, outfile,indent=4)

print("Task1 execution time:",time.time()-start)
