
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 18 01:00:25 2024

@author: altaf
"""

import os
import sys
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col
import json
from Levenshtein import distance
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


findspark.init()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("local[*]").appName("EntityResolution").getOrCreate()

df1 = spark.read.csv("C:/Users/altaf/Downloads/Truthorgi.csv", header=True, inferSchema=True)
df2 = spark.read.csv("C:/Users/altaf/Downloads/Easyorgi.csv", header=True, inferSchema=True)


df1 = df1.withColumn("source", col("TruthRowNum"))
df2 = df2.withColumn("source", col("TruthFileRowNum"))

combined_df = df1.select("SSN", "source").union(df2.select("SSN", "source"))

def map_function(row):
    ssn = row.SSN
    ids = [row.source]
    return ssn, ids

# Apply map function and reduceByKey
result_rdd = combined_df.rdd.map(map_function).reduceByKey(lambda x, y: x + y)

import json

# Convert RDD to dictionary
result_filtereds = result_rdd.collectAsMap()

# Convert dictionary to JSON format
json_data = json.dumps(result_filtereds, indent=2)

# Specify the output file path
output_path = "out.json"

# Write the JSON data to a file
with open(output_path, "w") as json_file:
    json_file.write(json_data)

print(f"Output saved to {output_path}")

# Print 5 SSNs with their associated IDs
for ssn, ids in result_rdd.take(5):
    print(f"SSN: {ssn}, IDs: {ids}")

spark.stop()

# Function to calculate similarity score between TID and EIDs based on first name
def calculate_score(tid, eids, df1, df2):
    tid_first_name = df1.filter(df1["source"] == tid).select("First").collect()[0][0]
    eid_first_names = [df2.filter(df2["source"] == eid).select("First Name").collect()[0][0] for eid in eids]
    
    scores = []
    for eid_first_name in eid_first_names:
        edit_distance = distance(tid_first_name.lower(), eid_first_name.lower())
        similarity_score = 1 - (edit_distance / max(len(tid_first_name), len(eid_first_name)))
        scores.append(similarity_score)
    
    return scores
id_pairs = []

# Iterate over the items in the dictionary
for ssn, ids in result_filtereds.items():
    # Extract IDs from truth and easy files
    truth_ids = [id_ for id_ in ids if id_.startswith("T")]
    easy_ids = [id_ for id_ in ids if id_.startswith("E")]

    # Generate pairs from truth to easy
    for truth_id in truth_ids:
        for easy_id in easy_ids:
            id_pairs.append((truth_id, easy_id))

output_file_path = "id_pairs.json"
id_pairs_set = set(id_pairs)
with open(output_file_path, "w") as json_file:
    json.dump(id_pairs, json_file, indent=4)   

print(f"ID pairs saved to {output_file_path}")
'''code for generating pairs ends'''
# Generate ID pairs with scores
id_pairs_with_scores = []

# Iterate through each SSN and its corresponding TIDs, EIDs
for ssn, ids in result_filtereds.items():
    tid = ids[0]  # TID is the first element
    eids = ids[1:]  # Rest are EIDs
    scores = calculate_score(tid, eids, df1, df2)
    id_pairs_with_scores.extend([(tid, eid, score) for eid, score in zip(eids, scores)])

# Save ID pairs with scores to a JSON file
output_file_path_with_scores = "new2.json"
with open(output_file_path_with_scores, "w") as json_file:
    json.dump(id_pairs_with_scores, json_file, indent=4)

print(f"ID pairs with scores saved to {output_file_path_with_scores}")



  
# Stop Spark session
spark.stop()
