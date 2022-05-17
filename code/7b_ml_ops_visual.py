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

## Part 7b - Model Operations - Visualising Model Metrics

# This is a continuation of the previous process started in the
# `7a_ml_ops_simulations.py` script.
# Here we will load in the metrics saved to the model database in the previous step
# into a Pandas dataframe, and display different features as graphs.

# ```python
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
# ```

import cdsw, time, os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import classification_report
import seaborn as sns
import sqlite3
import cmlapi
from src.api import ApiUtility

# You can access all models with API V2
client = cmlapi.default_client()

project_id = os.environ["CDSW_PROJECT_ID"]
client.list_models(project_id)

# You can use an APIV2-based utility to access the latest model's metadata. For more, explore the src folder
apiUtil = ApiUtility()

Model_CRN = apiUtil.get_latest_deployment_details(model_name="Churn Model API Endpoint")["model_crn"]
Deployment_CRN = apiUtil.get_latest_deployment_details(model_name="Churn Model API Endpoint")["latest_deployment_crn"]


# Read in the model metrics dict
model_metrics = cdsw.read_metrics(
    model_crn=Model_CRN, model_deployment_crn=Deployment_CRN
)

# This is a handy way to unravel the dict into a big pandas dataframe
metrics_df = pd.io.json.json_normalize(model_metrics["metrics"])
metrics_df.tail().T

# Write the data to SQL lite for visualization
if not (os.path.exists("model_metrics.db")):
    conn = sqlite3.connect("model_metrics.db")
    metrics_df.to_sql(name="model_metrics", con=conn)

# Do some conversions & calculations on the raw metrics
metrics_df["startTimeStampMs"] = pd.to_datetime(
    metrics_df["startTimeStampMs"], unit="ms"
)
metrics_df["endTimeStampMs"] = pd.to_datetime(metrics_df["endTimeStampMs"], unit="ms")
metrics_df["processing_time"] = (
    metrics_df["endTimeStampMs"] - metrics_df["startTimeStampMs"]
).dt.microseconds * 1000

# Create plots for different tracked metrics
sns.set_style("whitegrid")
sns.despine(left=True, bottom=True)

# Plot metrics.probability
prob_metrics = metrics_df.dropna(subset=["metrics.probability"]).sort_values(
    "startTimeStampMs"
)
sns.lineplot(
    x=range(len(prob_metrics)), y="metrics.probability", data=prob_metrics, color="grey"
)

# Plot processing time
time_metrics = metrics_df.dropna(subset=["processing_time"]).sort_values(
    "startTimeStampMs"
)
sns.lineplot(
    x=range(len(prob_metrics)), y="processing_time", data=prob_metrics, color="grey"
)

# Plot model accuracy drift over the simulated time period
agg_metrics = metrics_df.dropna(subset=["metrics.accuracy"]).sort_values(
    "startTimeStampMs"
)
sns.barplot(
    x=list(range(1, len(agg_metrics) + 1)),
    y="metrics.accuracy",
    color="grey",
    data=agg_metrics,
)
