# This script is executed in our Python Job (or Azure Data Factory Python Task)
# It can accept parameters
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import argparse
from importlib import import_module
from inspect import signature
import os
import json
import sys

spark = SparkSession.builder.appName('delta_App') \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

from delta.tables import *


# Also adding the modules hint so that we can execute this script locally
try:
    dirname = os.path.dirname(__file__)
    sys.path.insert(0, (os.path.join(dirname, 'pipelines')))
except:
    print('nothing')

# Define the parameters for our job
parser = argparse.ArgumentParser()
parser.add_argument("--etl", type=str, nargs='?', default="")
parser.add_argument("--customer", default=",")

# Parse the cmd line args
args = parser.parse_args()
args = vars(args)
# Convert to Json Dictionnary
argsJ = json.dumps(args, sort_keys=True, indent=4)
# Create a JSON Parser
argsJson = json.loads(s=argsJ)

try:
    etl = argsJson['etl']
    customer = argsJson['customer']
    print(etl)
    print(customer)
except expression as identifier:
    raise

#import module based on first parameter passed in
mod = import_module(etl, "pipelines")
met = getattr(mod, "etl")

#add Arguments
l = []
l.append(spark)
l.append(customer)
# Execute ETL
met(*l)

"""
etl = "pipelines.datasources.BIKE_RT"

#import module based on first parameter passed in
mod = import_module(etl, "pipelines")
met = getattr(mod, "etl")

# Get the parameters (if any) for the ETL
p = signature(met)
for a, b in p.parameters.items():
    parser.add_argument(b.name, type=str, nargs='?')

args = parser.parse_args()

# Loop through the arguements and pass to ETL (try casting to int)

"""