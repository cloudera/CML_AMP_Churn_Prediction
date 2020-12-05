## Part 7a - Model Operations - Drift Simulation
#
# This script show cases how to use the model operations features of CML.
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
# prediction and the acutal value (or lable) is only available at later stage. To check
# how well a model is performing, these actual values need to be compared with the 
# prediction the model. Each time a model end point is called, it provides the response
# from the function, some other details and a unique uuid for that response.
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
# the model less accurate. The actual value, the response value and the uuid are 
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
from cmlbootstrap import CMLBootstrap
import seaborn as sns
import copy


## Set the model ID
# Get the model id from the model you deployed in step 5. These are unique to each 
# model on CML.

model_id = "76"

# Grab the data from Hive.
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .master("local[*]")\
    .getOrCreate()

df = spark.sql("SELECT * FROM default.telco_churn").toPandas()

# Get the various Model CRN details
HOST = os.getenv("CDSW_API_URL").split(
    ":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split(
    "/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY") 
PROJECT_NAME = os.getenv("CDSW_PROJECT")  

cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

latest_model = cml.get_model({"id": model_id, "latestModelDeployment": True, "latestModelBuild": True})

Model_CRN = latest_model ["crn"]
Deployment_CRN = latest_model["latestModelDeployment"]["crn"]
model_endpoint = HOST.split("//")[0] + "//modelservice." + HOST.split("//")[1] + "/model"

# This will randomly return True for input and increases the likelihood of returning 
# true based on `percent`
def churn_error(item,percent):
  if random.random() < percent:
    return True
  else:
    return True if item=='Yes' else False

  
# Get 1000 samples  
df_sample = df.sample(1000)

df_sample.groupby('Churn')['Churn'].count() 

df_sample_clean = df_sample.\
  replace({'SeniorCitizen': {"1": 'Yes', "0": 'No'}}).\
  replace(r'^\s$', np.nan, regex=True).\
  dropna()

# Create an array of model responses.
response_labels_sample = []

# Make 1000 calls to the model with increasing error
percent_counter = 0
percent_max = len(df_sample_clean)

for record in json.loads(df_sample_clean.to_json(orient='records')):
  print("Added {} records".format(percent_counter)) if (percent_counter%50 == 0) else None
  percent_counter += 1
  no_churn_record = copy.deepcopy(record)
  no_churn_record.pop('customerID')
  no_churn_record.pop('Churn')
  # **note** this is an easy way to interact with a model in a script
  response = cdsw.call_model(latest_model["accessKey"],no_churn_record)
  response_labels_sample.append(
    {
      "uuid":response["response"]["uuid"],
      "final_label":churn_error(record["Churn"],percent_counter/percent_max),
      "response_label":response["response"]["prediction"]["probability"] >= 0.5,
      "timestamp_ms":int(round(time.time() * 1000))
    }
  )

# The "ground truth" loop adds the updated actual label value and an accuracy measure
# every 100 calls to the model.
for index, vals in enumerate(response_labels_sample):
  print("Update {} records".format(index)) if (index%50 == 0) else None  
  cdsw.track_delayed_metrics({"final_label":vals['final_label']}, vals['uuid'])
  if (index%100 == 0):
    start_timestamp_ms = vals['timestamp_ms']
    final_labels = []
    response_labels = []
  final_labels.append(vals['final_label'])
  response_labels.append(vals['response_label'])
  if (index%100 == 99):
    print("Adding accuracy metrc")
    end_timestamp_ms = vals['timestamp_ms']
    accuracy = classification_report(final_labels,response_labels,output_dict=True)["accuracy"]
    cdsw.track_aggregate_metrics({"accuracy": accuracy}, start_timestamp_ms , end_timestamp_ms, model_deployment_crn=Deployment_CRN)
