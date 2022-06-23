import os
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from mainsource.config import get_spark_config
from mainsource.tables import events_table
from mainsource.transformation import get_time_windows, get_per_window_agg, get_per_min_agg

# #creating the spark session with custom config
spark = SparkSession \
    .builder \
    .config(conf=get_spark_config(app_name="challenge 2")) \
    .getOrCreate()
spark.sparkContext.setLogLevel('warn')

logging.basicConfig(filename="custom_log.log",
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)

logging.info("Running spark Application")

# #reading the input files and validating the schema
input_file_path = os.path.join(Path(__file__).parent, events_table.input_path)

events_table.dataframe = spark.read.format(events_table.input_format) \
    .schema(events_table.schema).load(input_file_path)

# printing and validating parsed Dataframe
try:
    # events_table.dataframe_parsed.show()
    # print(events_table.dataframe_parsed.count())
    events_table.dataframe_parsed.printSchema()
except Exception as ex:
    raise Exception("Parsing error : File provided  could not be parsed with the provided schema"
                    f"----- underlying error ----- {ex}")

# getting the custome time windows of 10 and 1 min
events_table.dataframe_parsed = get_time_windows(events_table.dataframe_parsed)

# getting count per minute
agg_df = get_per_min_agg(events_table.dataframe_parsed)
agg_df.show()

# getting avg count per 10 min
agg_df2 = get_per_window_agg(agg_df)
agg_df2.show()

# getting the output path of the interim table and writing it to interim warehouse
events_table_file_path = os.path.join(Path(__file__).parent, events_table.output_path
                                      , events_table.output_name)
# for the sake of single file output converting to pandas otherwise can use spark writer to write to the disk
pd_agg_df = agg_df.toPandas()
pd_agg_df.to_csv(events_table_file_path + "_per_1_min." + "csv")

pd_agg_df = agg_df2.toPandas()
pd_agg_df.to_csv(events_table_file_path + "_per_10_min." + "csv")

#######################################
# 1. I have tried little modularisation so that unit test cases can be written on specific modules.
#    here we can write Unit test cases for all the transformation submodule. because of time constraint
#    I am skipping this.
# 2. before production deployment we should create a CI/CD pipeline using shell and python.
#       we can run these in the cloud run, github actions etc.
# 3. these deployment actions will consist of multiple procedures like
#           * Linting and code scoring
#           * Unit test cases run using unittest or pytest
#           * Integration test runs
#           * creating the build and pushing it to artifactory
#           * creating an image and deploying it to kubernet cluster.
# 4. Integration testing: for this we need to parameterize this job by providing the parameters like
#                     deployment type(testing, release, debugging etc). following these parameters job will identify
#                     the input and output path from the configuration and run an integration test by asserting the
#                     final output. we can also parameterize input and output paths but that will simply
#                     increase the reliance of accuracy at deployment job. Instead, we will configure this in
#                     job configuration and it will decide the next process. if its testing, then we will code it to
#                     take pre-defined input like table, parquet or csv and run whole pipeline consuming it. and the
#                     end results will be compared with the expected output and red flag will be raised accordingly.
#                     this can be articulated differently by different engineer. but the gist is we are running the
#                     pipeline in the emulated production environment with prescribed input and  expected output
#
