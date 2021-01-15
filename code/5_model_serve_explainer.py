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

## Part 5: Model Serving
#
# This notebook explains how to create and deploy Models in CML which function as a
# REST API to serve predictions. This feature makes it very easy for a data scientist
# to make trained models available and usable to other developers and data scientists
# in your organization.
#
# In the last part of the series, you learned:
# - the requirements for running an Experiment
# - how to set up a new Experiment
# - how to monitor the results of an Experiment
# - limitations of the feature
#
# In this part, you will learn:
# - the requirements for creating and deploying a Model
# - how to deploy a Model
# - how to test and use a Model
# - limitations of the feature
#
# If you haven't yet, run through the initialization steps in the README file and Part 1.
# In Part 1, the data is imported into the table you specified in Hive.
# All data accesses fetch from Hive.
#
### Requirements
# Models have the same requirements as Experiments:
# - model code in a `.py` script, not a notebook
# - a `requirements.txt` file listing package dependencies
# - a `cdsw-build.sh` script containing code to install all dependencies
#
# > In addition, Models *must* be designed with one main function that takes a dictionary as its sole argument
# > and returns a single dictionary.
# > CML handles the JSON serialization and deserialization.

# In this file, there is minimal code since calculating predictions is much simpler
# than training a machine learning model.
# Once again, we use the `ExplainedModel` helper class in `churnexplainer.py`.
# When a Model API is called, CML will translate the input and returned JSON blobs to and from python dictionaries.
# Thus, the script simply loads the model we saved at the end of the last notebook,
# passes the input dictionary into the model, and returns the results as a dictionary with the following format:
#
#    {
#        'data': dict(data),
#        'probability': probability,
#        'explanation': explanation
#    }
#
# The Model API will return this dictionary serialized as JSON.
#
### Model Operations
#
# This model is deployed using the model operations feature of CML which consists of
# [Model Metrics](https://docs.cloudera.com/machine-learning/cloud/model-metrics/topics/ml-enabling-model-metrics.html)
# and [Model Governance](https://docs.cloudera.com/machine-learning/cloud/model-governance/topics/ml-enabling-model-governance.html)
#
# The first requirement to make the model use the model metrics feature by adding the
# `@cdsw.model_metrics` [Python Decorator](https://wiki.python.org/moin/PythonDecorators)
# before the fuction.
#
# Then you can use the *`cdsw.track_metric`* function to add additional
# data to the underlying database for each call made to the model.
# **Note:** `cdsw.track_metric` has different functionality depening on if its being
# used in an *Experiment* or a *Model*.
#
# More detail is available
# using the `help(cdsw.track_mertic)` function
# ```
# help(cdsw.track_metric)
# Help on function track_metric in module cdsw:
#
# track_metric(key, value)
#    Description
#    -----------
#
#    Tracks a metric for an experiment or model deployment
#        Example:
#            model deployment usage:
#                >>>@cdsw.model_metrics
#                >>>predict_func(args):
#                >>>   cdsw.track_metric("input_args", args)
#                >>>   return {"result": "prediction"}
#
#            experiment usage:
#                >>>cdsw.track_metric("input_args", args)
#
#    Parameters
#    ----------
#    key: string
#        The metric key to track
#    value: string, boolean, numeric
#        The metric value to track
# ```
#
#
### Creating and deploying a Model
# To create a Model using our `5_model_serve_explainer.py` script, use the following settings:
# * **Name**: Explainer
# * **Description**: Explain customer churn prediction
# * **File**: `5_model_serve_explainer.py`
# * **Function**: explain
# * **Input**:
# ```
# {
# 	"StreamingTV": "No",
# 	"MonthlyCharges": 70.35,
# 	"PhoneService": "No",
# 	"PaperlessBilling": "No",
# 	"Partner": "No",
# 	"OnlineBackup": "No",
# 	"gender": "Female",
# 	"Contract": "Month-to-month",
# 	"TotalCharges": 1397.475,
# 	"StreamingMovies": "No",
# 	  "DeviceProtection": "No",
# 	  "PaymentMethod": "Bank transfer (automatic)",
# 	  "tenure": 29,
# 	  "Dependents": "No",
# 	  "OnlineSecurity": "No",
# 	  "MultipleLines": "No",
# 	  "InternetService": "DSL",
# 	  "SeniorCitizen": "No",
# 	  "TechSupport": "No"
# }
# ```
# * **Kernel**: Python 3
# * **Engine Profile**: 1 vCPU / 2 GiB Memory
#
# The rest can be left as is.
#
# After accepting the dialog, CML will *build* a new Docker image using `cdsw-build.sh`,
# then *assign an endpoint* for sending requests to the new Model.

## Testing the Model
# > To verify it's returning the right results in the format you expect, you can
# > test any Model from it's *Overview* page.
#
# If you entered an *Example Input* before, it will be the default input here,
# though you can enter your own.

## Using the Model
#
# > The *Overview* page also provides sample `curl` or Python commands for calling your Model API.
# > You can adapt these samples for other code that will call this API.
#
# This is also where you can find the full endpoint to share with other developers
# and data scientists.
#
# **Note:** for security, you can specify
# [Model API Keys](https://docs.cloudera.com/machine-learning/cloud/models/topics/ml-model-api-key-for-models.html)
# to add authentication.

## Limitations
#
# Models do have a few limitations that are important to know:
# - re-deploying or re-building Models results in Model downtime (usually brief)
# - re-starting CML does not automatically restart active Models
# - Model logs and statistics are only preserved so long as the individual replica is active
#
# A current list of known limitations are
# [documented here](https://docs.cloudera.com/machine-learning/cloud/models/topics/ml-models-known-issues-and-limitations.html).


from collections import ChainMap
import cdsw, numpy
from churnexplainer import ExplainedModel

# Load the model saved earlier.
em = ExplainedModel(model_name="telco_linear", data_dir="/home/cdsw")

# *Note:* If you want to test this in a session, comment out the line
# `@cdsw.model_metrics` below. Don't forget to uncomment when you
# deploy, or it won't write the metrics to the database


@cdsw.model_metrics
# This is the main function used for serving the model. It will take in the JSON formatted arguments , calculate the probablity of
# churn and create a LIME explainer explained instance and return that as JSON.
def explain(args):
    data = dict(ChainMap(args, em.default_data))
    data = em.cast_dct(data)
    probability, explanation = em.explain_dct(data)

    # Track inputs
    cdsw.track_metric("input_data", data)

    # Track our prediction
    cdsw.track_metric("probability", probability)

    # Track explanation
    cdsw.track_metric("explanation", explanation)

    return {"data": dict(data), "probability": probability, "explanation": explanation}


# To test this is a session, comment out the `@cdsw.model_metrics`  line,
# uncomment the and run the two rows below.
# x={"StreamingTV":"No","MonthlyCharges":70.35,"PhoneService":"No","PaperlessBilling":"No","Partner":"No","OnlineBackup":"No","gender":"Female","Contract":"Month-to-month","TotalCharges":1397.475,"StreamingMovies":"No","DeviceProtection":"No","PaymentMethod":"Bank transfer (automatic)","tenure":29,"Dependents":"No","OnlineSecurity":"No","MultipleLines":"No","InternetService":"DSL","SeniorCitizen":"No","TechSupport":"No"}
# explain(x)

## Wrap up
#
# We've now covered all the steps to **deploying and serving Models**, including the
# requirements, limitations, and how to set up, test, and use them.
# This is a powerful way to get data scientists' work in use by other people quickly.
#
# In the next part of the project we will explore how to launch a **web application**
# served through CML.
# Your team is busy building models to solve problems.
# CML-hosted Applications are a simple way to get these solutions in front of
# stakeholders quickly.
