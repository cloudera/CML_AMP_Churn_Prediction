## Part 7b - Model Operations - Visualising Model Metrics

# This is a continuation of the previous process started in the 
# `7a_ml_ops_simulations.py` script.
# Here we will load in the metrics saved to the model database in the previous step 
# into a Pandas dataframe, and display different features as graphs. 

#```python
# help(cdsw.read_metrics)
# Help on function read_metrics in module cdsw:
#
# read_metrics(model_deployment_crn=None, start_timestamp_ms=None, end_timestamp_ms=None, model_crn=None, model_build_crn=None)
#    Description
#    -----------
#    
#    Read metrics data for given Crn with start and end time stamp
#    
#    Parameters
#    ----------
#    model_deployment_crn: string
#        model deployment Crn
#    model_crn: string
#        model Crn
#    model_build_crn: string
#        model build Crn
#    start_timestamp_ms: int, optional
#        metrics data start timestamp in milliseconds , if not passed
#        default value 0 is used to fetch data
#    end_timestamp_ms: int, optional
#        metrics data end timestamp in milliseconds , if not passed
#        current timestamp is used to fetch data
#    
#    Returns
#    -------
#    object
#        metrics data
#```
 

import cdsw, time, os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import classification_report
from cmlbootstrap import CMLBootstrap
import seaborn as sns
import sqlite3


## Set the model ID
# Get the model id from the model you deployed in step 5. These are unique to each 
# model on CML.

model_id = "76"

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

# Read in the model metrics dict.
model_metrics = cdsw.read_metrics(model_crn=Model_CRN,model_deployment_crn=Deployment_CRN)

# This is a handy way to unravel the dict into a big pandas dataframe.
metrics_df = pd.io.json.json_normalize(model_metrics["metrics"])
metrics_df.tail().T

# Write the data to SQL lite for Viz Apps
if not(os.path.exists("model_metrics.db")):
  conn = sqlite3.connect('model_metrics.db')
  metrics_df.to_sql(name='model_metrics', con=conn)

# Do some conversions & calculations
metrics_df['startTimeStampMs'] = pd.to_datetime(metrics_df['startTimeStampMs'], unit='ms')
metrics_df['endTimeStampMs'] = pd.to_datetime(metrics_df['endTimeStampMs'], unit='ms')
metrics_df["processing_time"] = (metrics_df["endTimeStampMs"] - metrics_df["startTimeStampMs"]).dt.microseconds * 1000

# This shows how to plot specific metrics.
sns.set_style("whitegrid")
sns.despine(left=True,bottom=True)

prob_metrics = metrics_df.dropna(subset=['metrics.probability']).sort_values('startTimeStampMs')
sns.lineplot(x=range(len(prob_metrics)), y="metrics.probability", data=prob_metrics, color='grey')

time_metrics = metrics_df.dropna(subset=['processing_time']).sort_values('startTimeStampMs')
sns.lineplot(x=range(len(prob_metrics)), y="processing_time", data=prob_metrics, color='grey')

# This shows how the model accuracy drops over time.
agg_metrics = metrics_df.dropna(subset=["metrics.accuracy"]).sort_values('startTimeStampMs')
sns.barplot(x=list(range(1,len(agg_metrics)+1)), y="metrics.accuracy", color="grey", data=agg_metrics)
