# Part 1: Data Ingest
# A data scientist should never be blocked in getting data into their environment,
# so CML is able to ingest data from many sources.
# Whether you have data in .csv files, modern formats like parquet or feather,
# in cloud storage or a SQL database, CML will let you work with it in a data
# scientist-friendly environment.

# Access local data on your computer
#
# Accessing data stored on your computer is a matter of [uploading a file to the CML filesystem and
# referencing from there](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-accessing-local-data-from-your-computer.html).
#
# > Go to the project's **Overview** page. Under the **Files** section, click **Upload**, select the relevant data files to be uploaded and a destination folder.
#
# If, for example, you upload a file called, `mydata.csv` to a folder called `data`, the
# following example code would work.

# ```
# import pandas as pd
#
# df = pd.read_csv('data/mydata.csv')
#
# # Or:
# df = pd.read_csv('/home/cdsw/data/mydata.csv')
# ```

# Access data in S3
#
# Accessing [data in Amazon S3](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-accessing-data-in-amazon-s3-buckets.html)
# follows a familiar procedure of fetching and storing in the CML filesystem.
# > Add your Amazon Web Services access keys to your project's
# > [environment variables](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-environment-variables.html)
# > as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
#
# To get the the access keys that are used for you in the CDP DataLake, you can follow
# [this Cloudera Community Tutorial](https://community.cloudera.com/t5/Community-Articles/How-to-get-AWS-access-keys-via-IDBroker-in-CDP/ta-p/295485)

#
# The following sample code would fetch a file called `myfile.csv` from the S3 bucket, `data_bucket`, and store it in the CML home folder.
# ```
# # Create the Boto S3 connection object.
# from boto.s3.connection import S3Connection
# aws_connection = S3Connection()
#
# # Download the dataset to file 'myfile.csv'.
# bucket = aws_connection.get_bucket('data_bucket')
# key = bucket.get_key('myfile.csv')
# key.get_contents_to_filename('/home/cdsw/myfile.csv')
# ```


# Access data from Cloud Storage or the Hive metastore
#
# Accessing data from [the Hive metastore](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-accessing-data-from-apache-hive.html)
# that comes with CML only takes a few more steps.
# But first we need to fetch the data from Cloud Storage and save it as a Hive table.
#
# > First we specify `STORAGE` as an
# > [environment variable](https://docs.cloudera.com/machine-learning/cloud/import-data/topics/ml-environment-variables.html)
# > in your project settings containing the Cloud Storage location used by the DataLake to store
# > Hive data. On AWS it will be `s3a://[something]`, on Azure it will be `abfs://[something]` and on
# > on prem CDSW cluster, it will be `hdfs://[something]`
#
# This was done for you when you ran `0_bootstrap.py`, so the following code is set up to run as is.
# It begins with imports and creating a `SparkSession`.

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *



spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .master("local[*]")\
    .getOrCreate()

# **Note:**
# Our file isn't big, so running it in Spark local mode is fine but you can add the following config
# if you want to run Spark on the kubernetes cluster
#
# > .config("spark.yarn.access.hadoopFileSystems",os.getenv['STORAGE'])\
#
# and remove `.master("local[*]")\`
#

# Since we know the data already, we can add schema upfront. This is good practice as Spark will
# read *all* the Data if you try infer the schema.

schema = StructType(
    [
        StructField("customerID", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("SeniorCitizen", StringType(), True),
        StructField("Partner", StringType(), True),
        StructField("Dependents", StringType(), True),
        StructField("tenure", DoubleType(), True),
        StructField("PhoneService", StringType(), True),
        StructField("MultipleLines", StringType(), True),
        StructField("InternetService", StringType(), True),
        StructField("OnlineSecurity", StringType(), True),
        StructField("OnlineBackup", StringType(), True),
        StructField("DeviceProtection", StringType(), True),
        StructField("TechSupport", StringType(), True),
        StructField("StreamingTV", StringType(), True),
        StructField("StreamingMovies", StringType(), True),
        StructField("Contract", StringType(), True),
        StructField("PaperlessBilling", StringType(), True),
        StructField("PaymentMethod", StringType(), True),
        StructField("MonthlyCharges", DoubleType(), True),
        StructField("TotalCharges", DoubleType(), True),
        StructField("Churn", StringType(), True)
    ]
)

# Now we can read in the data from Cloud Storage into Spark...

storage = os.environ['STORAGE']

telco_data = spark.read.csv(
    "{}/datalake/data/churn/WA_Fn-UseC_-Telco-Customer-Churn-.csv".format(
        storage),
    header=True,
    schema=schema,
    sep=',',
    nullValue='NA'
)

# ...and inspect the data.

telco_data.show()

telco_data.printSchema()

# Now we can store the Spark DataFrame as a file in the local CML file system
# *and* as a table in Hive used by the other parts of the project.

telco_data.coalesce(1).write.csv(
    "file:/home/cdsw/raw/telco-data/",
    mode='overwrite',
    header=True
)

spark.sql("show databases").show()

spark.sql("show tables in default").show()

# Create the Hive table
# This is here to create the table in Hive used be the other parts of the project, if it
# does not already exist.

if ('telco_churn' not in list(spark.sql("show tables in default").toPandas()['tableName'])):
    print("creating the telco_churn database")
    telco_data\
        .write.format("parquet")\
        .mode("overwrite")\
        .saveAsTable(
            'default.telco_churn'
        )

# Show the data in the hive table
spark.sql("select * from default.telco_churn").show()

# To get more detailed information about the hive table you can run this:
spark.sql("describe formatted default.telco_churn").toPandas()

# Other ways to access data

# To access data from other locations, refer to the
# [CML documentation](https://docs.cloudera.com/machine-learning/cloud/import-data/index.html).

# Scheduled Jobs
#
# One of the features of CML is the ability to schedule code to run at regular intervals,
# similar to cron jobs. This is useful for **data pipelines**, **ETL**, and **regular reporting**
# among other use cases. If new data files are created regularly, e.g. hourly log files, you could
# schedule a Job to run a data loading script with code like the above.

# > Any script [can be scheduled as a Job](https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-job.html).
# > You can create a Job with specified command line arguments or environment variables.
# > Jobs can be triggered by the completion of other jobs, forming a
# > [Pipeline](https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-pipeline.html)
# > You can configure the job to email individuals with an attachment, e.g. a csv report which your
# > script saves at: `/home/cdsw/job1/output.csv`.

# Try running this script `1_data_ingest.py` for use in such a Job.
