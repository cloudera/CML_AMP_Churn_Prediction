# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2021
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# ###########################################################################

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
import subprocess

from cmlbootstrap import CMLBootstrap
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *

# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY")
PROJECT_NAME = os.getenv("CDSW_PROJECT")

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# Set the STORAGE environment variable
# TODO: this code block is repeated from 0_bootstrap.py -- fix this
try:
    storage = os.environ["STORAGE"]
except:
    # set the default external storage location 
    storage = "/user/" + os.getenv("HADOOP_USER_NAME") 
    
    # if avaiable, set the external storage location for PbC
    if os.path.exists("/etc/hadoop/conf/hive-site.xml"):
        tree = ET.parse("/etc/hadoop/conf/hive-site.xml")
        root = tree.getroot()
        for prop in root.findall("property"):
            if prop.find("name").text == "hive.metastore.warehouse.dir":
                # catch erroneous pvc external storage locale
                if len(prop.find("value").text.split("/")) > 5:
                    storage = (
                        prop.find("value").text.split("/")[0]
                        + "//"
                        + prop.find("value").text.split("/")[2]
                    )

    # create and set storage environment variables
    storage_environment = cml.create_environment_variable({"STORAGE": storage})
    os.environ["STORAGE"] = storage
    


spark = SparkSession.builder.appName("PythonSQL").master("local[*]").getOrCreate()

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
        StructField("Churn", StringType(), True),
    ]
)

# Now we can read in the data into Spark
storage = os.environ["STORAGE"]
data_location = os.environ["DATA_LOCATION"]
hive_database = os.environ["HIVE_DATABASE"]
hive_table = os.environ["HIVE_TABLE"]
hive_table_fq = hive_database + "." + hive_table

if os.environ["STORAGE_MODE"] == "external":
    path = f"{storage}/{data_location}/WA_Fn-UseC_-Telco-Customer-Churn-.csv"
else:
    path = "/home/cdsw/raw/WA_Fn-UseC_-Telco-Customer-Churn-.csv"

if os.environ["STORAGE_MODE"] == "external":
    
    try:
        telco_data = spark.read.csv(path, header=True, schema=schema, sep=",", nullValue="NA")

        # ...and inspect the data.
        telco_data.show()

        telco_data.printSchema()

        # Now we can store the Spark DataFrame as a file in the local CML file system
        # *and* (if possible) as a table in Hive used by the other parts of the project.
        telco_data.coalesce(1).write.csv(
            "file:/home/cdsw/raw/telco-data/", mode="overwrite", header=True
        )
        spark.sql("show databases").show()
        spark.sql("show tables in " + hive_database).show()

        # Create the Hive table, if possible
        # This is here to create the table in Hive used be the other parts of the project, if it
        # does not already exist.
        if hive_table not in list(
            spark.sql("show tables in " + hive_database).toPandas()["tableName"]
        ):
            print("creating the " + hive_table + " table")

            try:
                telco_data.write.format("parquet").mode("overwrite").saveAsTable(
                    hive_table_fq
                )
            except AnalysisException as ae:
                print(ae)
                print("Removing the conflicting directory from storage location.")

                conflict_location = f'{os.environ["STORAGE"]}/datalake/data/warehouse/tablespace/external/hive/{os.environ["HIVE_TABLE"]}'
                cmd = ["hdfs", "dfs", "-rm", "-r", conflict_location]
                subprocess.call(cmd)
                telco_data.write.format("parquet").mode("overwrite").saveAsTable(
                    hive_table_fq
                )

            # Show the data in the hive table
            spark.sql("select * from " + hive_table_fq).show()

            # To get more detailed information about the hive table you can run this:
            spark.sql("describe formatted " + hive_table_fq).toPandas()

    except Exception as e:
        print(e)
        print("Continuing AMP in STORAGE_MODE == local")
        cml.create_environment_variable({"STORAGE_MODE": "local"})


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