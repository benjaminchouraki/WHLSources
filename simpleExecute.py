
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

spark = SparkSession.builder.appName('delta_App') \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

from delta.tables import *
from pipelines.datasources import TRAF_EVENT, PARK_RT, MAP, POI, BIKE_RT, CARSH_RT, OFFRE_TC, ACTIVITY, TRAF_SPEED
from pipelines.utils.deltaHelper import createTable

# Use this script to test executing the pipleines locally


#TRAF_EVENT.etl(spark)
# MAP.etl(spark)
# POI.etl(spark)
#PARK_RT.etl(spark)
# BIKE_RT.etl(spark)
# CARSH_RT.etl(spark)
# OFFRE_TC.etl(spark)
# ACTIVITY.etl(spark)
# TRAF_SPEED.etl(spark)

createTable(spark, "MaaS_Analytics", "PARK_RT", "output-files/PARK_RT")
df = spark.sql("SELECT * FROM MaaS_Analytics.PARK_RT WHERE ParkingId = '1780'")
df.show(truncate=False)