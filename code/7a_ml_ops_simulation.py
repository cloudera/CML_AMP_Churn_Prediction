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

## Part 7a - Model Operations - Drift Simulation
#
# This script showcases how to use the model operations features of CML.
# # This feature allows machine learning engineering to **measure and manage models
# through their life cycle**, and know how a model is performing over time. As part
# of the larger machine learning lifecycle, this closes the loop on managing
# models that have been deployed into production.

### Add Model Metrics
# New  metrics can be added to a model and existing ones updated using the `cdsw`
# library and the [model metrics SDK](https://docs.cloudera.com/machine-learning/cloud/model-metrics/topics/ml-tracking-model-metrics-using-python.html)
# If model metrics is enabled for a model, then every call to that model is recorded
# in the model metric database. There are situations in which its necessary to update or
# add to those recordered metrics. This script shows you how this works.

#### Update Exsiting Tracked Metrics
# This is part of what is called "ground truth". Certain machine learning implemetations,
# (like this very project) will use a supervised approach where a model is making a
# prediction and the acutal value (or label) is only available at a later stage. To check
# how well a model is performing, these actual values need to be compared with the
# prediction from the model. Each time a model endpoint is called, it provides the response
# from the function, some other details, and a unique uuid for that response.
# This tracked model response entry can then be updated at a later date to add the
# actual "ground truth" value, or any other data that you want to add.
#
# Data can be added to a tracked model response using the `cdsw.track_delayed_metrics`.
#
# ```python
# help(cdsw.track_delayed_metrics)
# Help on function track_delayed_metrics in module cdsw:
#
# track_delayed_metrics(metrics, prediction_uuid)
#    Description
#    -----------
#
#    Track a metric for a model prediction that is only known after prediction time.
#    For example, for a model that makes a binary or categorical prediction, the actual
#    correctness of the prediction is not known at prediction time. This function can be
#    used to retroactively to track a prediction's correctness later, when ground truth
#    is available
#        Example:
#            >>>track_delayed_metrics({"ground_truth": "value"}, "prediction_uuid")
#
#    Parameters
#    ----------
#    metrics: object
#        metrics object
#    prediction_uuid: string, UUID
#        prediction UUID of model metrics
# ```

#### Adding Additional Metrics
# It is also possible to add additional data/metrics to the model database to track
# things like aggrerate metrics that aren't associated with the one particular response.
# This can be done using the `cdsw.track_aggregate_metrics` function.

# ```python
# help(cdsw.track_aggregate_metrics)
# Help on function track_aggregate_metrics in module cdsw:
#
# track_aggregate_metrics(metrics, start_timestamp_ms, end_timestamp_ms, model_deployment_crn=None)
#    Description
#    -----------
#
#    Track aggregate metric data for model deployment or model build or model
#        Example:
#            >>>track_aggregate_metrics({"val_count": 125}, 1585685142786,
#            ... 1585685153602, model_deployment_crn="/db401b6a-4b26-4c8f-8ea6-a1b09b93db88"))
#
#    Parameters
#    ----------
#    metrics: object
#        metrics data object
#    start_timestamp_ms: int
#        aggregated metrics start timestamp in milliseconds
#    end_timestamp_ms: int
#        aggregated metrics end timestamp in milliseconds
#    model_deployment_crn: string
#       model deployment Crn
# ```
#

### Model Drift Simlation
# This script simulates making calls to the model using sample data, and slowly
# introducting an increasing amount of random variation to the churn value so that
# the model will be less accurate over time.

# The script will grab 1000 random samples from the data set and simulate 1000
# predictions. The live model will be called each time in the loop and while the
# `churn_error` function adds an increasing amount of error to the data to make
# the model less accurate. The actual value, the response value, and the uuid are
# added to an array.
#
# Then there is "ground truth" loop that iterates though the array and updates the
# recorded metric to add the actual lable value using the uuid. At the same time, the
# model accruacy is evaluated every 100 samples and added as an aggregate metric.
# Overtime this accuracy metric falls due the error introduced into the data.


import cdsw, time, os, random, json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import classification_report
import seaborn as sns
import copy
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import cmlapi
from src.api import ApiUtility

hive_database = os.environ["HIVE_DATABASE"]
hive_table = os.environ["HIVE_TABLE"]
hive_table_fq = hive_database + "." + hive_table

# read data into a Spark DataFrame
spark = SparkSession.builder.appName("PythonSQL").master("local[*]").getOrCreate()

if os.environ["STORAGE_MODE"] == "external":
    telco_data_raw = spark.sql("SELECT * FROM " + hive_table_fq)
else:
    path = "/home/cdsw/raw/WA_Fn-UseC_-Telco-Customer-Churn-.csv"
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
    telco_data_raw = spark.read.csv(
        path, header=True, sep=",", schema=schema, nullValue="NA"
    )
df = telco_data_raw.toPandas()

# You can access all models with API V2
client = cmlapi.default_client()
project_id = os.environ["CDSW_PROJECT_ID"]
client.list_models(project_id)

# You can use an APIV2-based utility to access the latest model's metadata. For more, explore the src folder
apiUtil = ApiUtility()
Model_AccessKey = apiUtil.get_latest_deployment_details(model_name="Churn Model API Endpoint")["model_access_key"]
Deployment_CRN = apiUtil.get_latest_deployment_details(model_name="Churn Model API Endpoint")["latest_deployment_crn"]


# This will randomly return True for input and increases the likelihood of returning
# true based on `percent`
def churn_error(item, percent):
    if random.random() < percent:
        return True
    else:
        return True if item == "Yes" else False

# Get 1000 samples
df_sample = df.sample(1000)
df_sample.groupby("Churn")["Churn"].count()
df_sample_clean = (
    df_sample.replace({"SeniorCitizen": {"1": "Yes", "0": "No"}})
    .replace(r"^\s$", np.nan, regex=True)
    .dropna()
)

# Create an array of model responses.
response_labels_sample = []

# Run Similation to make 1000 calls to the model with increasing error
percent_counter = 0
percent_max = len(df_sample_clean)

for record in json.loads(df_sample_clean.to_json(orient="records")):
    print("Added {} records".format(percent_counter)) if (
        percent_counter % 50 == 0
    ) else None
    percent_counter += 1
    no_churn_record = copy.deepcopy(record)
    no_churn_record.pop("customerID")
    no_churn_record.pop("Churn")
    # **note** this is an easy way to interact with a model in a script
    response = cdsw.call_model(Model_AccessKey, no_churn_record)
    response_labels_sample.append(
        {
            "uuid": response["response"]["uuid"],
            "final_label": churn_error(record["Churn"], percent_counter / percent_max),
            "response_label": response["response"]["prediction"]["probability"] >= 0.5,
            "timestamp_ms": int(round(time.time() * 1000)),
        }
    )

# The "ground truth" loop adds the updated actual label value and an accuracy measure
# every 100 calls to the model.
for index, vals in enumerate(response_labels_sample):
    print("Update {} records".format(index)) if (index % 50 == 0) else None
    cdsw.track_delayed_metrics({"final_label": vals["final_label"]}, vals["uuid"])
    if index % 100 == 0:
        start_timestamp_ms = vals["timestamp_ms"]
        final_labels = []
        response_labels = []
    final_labels.append(vals["final_label"])
    response_labels.append(vals["response_label"])
    if index % 100 == 99:
        print("Adding accuracy metrc")
        end_timestamp_ms = vals["timestamp_ms"]
        accuracy = classification_report(
            final_labels, response_labels, output_dict=True
        )["accuracy"]
        cdsw.track_aggregate_metrics(
            {"accuracy": accuracy},
            start_timestamp_ms,
            end_timestamp_ms,
            model_deployment_crn=Deployment_CRN,
        )
