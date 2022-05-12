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

# Part 4: Model Training

# This script is used to train an Explained model using the Jobs feature
# in CML and the Experiments feature to facilitate model tuning

# If you haven't yet, run through the initialization steps in the README file and Part 1.
# In Part 1, the data is imported into the table you specified in Hive.
# All data accesses fetch from Hive.
#
# To simply train the model once, run this file in a workbench session.
#
# There are 2 other ways of running the model training process
#
# ***Scheduled Jobs***
#
# The **[Jobs](https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-job.html)**
# feature allows for adhoc, recurring and depend jobs to run specific scripts. To run this model
# training process as a job, create a new job by going to the Project window and clicking _Jobs >
# New Job_ and entering the following settings:
# * **Name** : Train Mdoel
# * **Script** : 4_train_models.py
# * **Arguments** : _Leave blank_
# * **Kernel** : Python 3
# * **Schedule** : Manual
# * **Engine Profile** : 1 vCPU / 2 GiB
# The rest can be left as is. Once the job has been created, click **Run** to start a manual
# run for that job.

# ***Experiments***
#
# Training a model for use in production requires testing many combinations of model parameters
# and picking the best one based on one or more metrics.
# In order to do this in a *principled*, *reproducible* way, an Experiment executes model training code with **versioning** of the **project code**, **input parameters**, and **output artifacts**.
# This is a very useful feature for testing a large number of hyperparameters in parallel on elastic cloud resources.

# **[Experiments](https://docs.cloudera.com/machine-learning/cloud/experiments/topics/ml-running-an-experiment.html)**.
# run immediately and are used for testing different parameters in a model training process.
# In this instance it would be use for hyperparameter optimisation. To run an experiment, from the
# Project window click Experiments > Run Experiment with the following settings.
# * **Script** : 4_train_models.py
# * **Arguments** : 5 lbfgs 100 _(these the cv, solver and max_iter parameters to be passed to
# LogisticRegressionCV() function)
# * **Kernel** : Python 3
# * **Engine Profile** : 1 vCPU / 2 GiB

# Click **Start Run** and the expriment will be sheduled to build and run. Once the Run is
# completed you can view the outputs that are tracked with the experiment using the
# `cdsw.track_metrics` function. It's worth reading through the code to get a sense of what
# all is going on.

# More Details on Running Experiments
# Requirements
# Experiments have a few requirements:
# - model training code in a `.py` script, not a notebook
# - `requirements.txt` file listing package dependencies
# - a `cdsw-build.sh` script containing code to install all dependencies
#
# These three components are provided for the churn model as `4_train_models.py`, `requirements.txt`,
# and `cdsw-build.sh`, respectively.
# You can see that `cdsw-build.sh` simply installs packages from `requirements.txt`.
# The code in `4_train_models.py` is largely identical to the code in the last notebook.
# with a few differences.
#
# The first difference from the last notebook is at the "Experiments options" section.
# When you set up a new Experiment, you can enter
# [**command line arguments**](https://docs.python.org/3/library/sys.html#sys.argv)
# in standard Python fashion.
# This will be where you enter the combination of model hyperparameters that you wish to test.
#
# The other difference is at the end of the script.
# Here, the `cdsw` package (available by default) provides
# [two methods](https://docs.cloudera.com/machine-learning/cloud/experiments/topics/ml-tracking-metrics.html)
# to let the user evaluate results.
#
# **`cdsw.track_metric`** stores a single value which can be viewed in the Experiments UI.
# Here we store two metrics and the filepath to the saved model.
#
# **`cdsw.track_file`** stores a file for later inspection.
# Here we store the saved model, but we could also have saved a report csv, plot, or any other
# output file.
#


from pyspark.sql.types import *
from pyspark.sql import SparkSession
import sys
import os
import pandas as pd
import numpy as np
import cdsw
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegressionCV
from sklearn.compose import ColumnTransformer
from lime.lime_tabular import LimeTabularExplainer

try:
  os.chdir("code")
except:
  pass
from churnexplainer import ExplainedModel, CategoricalEncoder

hive_database = os.environ["HIVE_DATABASE"]
hive_table = os.environ["HIVE_TABLE"]
hive_table_fq = hive_database + "." + hive_table

data_dir = "/home/cdsw"
labelcol = "Churn"


# This is a fail safe incase the hive table did not get created in the last step.
try:
    spark = SparkSession.builder.appName("PythonSQL").master("local[*]").getOrCreate()

    if spark.sql("SELECT count(*) FROM " + hive_table_fq).collect()[0][0] > 0:
        df = spark.sql("SELECT * FROM " + hive_table_fq).toPandas()
except:
    print("Hive table has not been created")
    df = pd.read_csv(os.path.join("../raw", "WA_Fn-UseC_-Telco-Customer-Churn-.csv"))


# Clean and prep the dataframe
df = (df
      .replace(r"^\s$", np.nan, regex=True).dropna().reset_index()
      # drop unnecessary and personally identifying information
      .drop(columns=['index', 'customerID'])
      .replace({"SeniorCitizen": {1: "Yes", 0: "No", "1": "Yes", "0": "No"}})
     )
df['TotalCharges'] = df['TotalCharges'].astype('float')
df.index.name='id'


# separate target variable column from feature columns
datadf, labels = df.drop(labelcol, axis=1), df[labelcol]

# recast all columns that are "object" dtypes to Categorical
for colname, dtype in zip(datadf.columns, datadf.dtypes):
  if dtype == "object":
    datadf[colname] = pd.Categorical(datadf[colname])

  
# Prepare data for Sklearn model and create train/test split
ce = CategoricalEncoder()
X = ce.fit_transform(datadf)
y = labels.values
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42)
ct = ColumnTransformer(
    [("ohe", OneHotEncoder(), list(ce.cat_columns_ix_.values()))],
    remainder="passthrough",
)

# Experiments options
# If you are running this as an experiment, pass the cv, solver and max_iter values
# as arguments in that order. e.g. `5 lbfgs 100`.
if len(sys.argv) == 4:
    try:
        cv = int(sys.argv[1])
        solver = str(sys.argv[2])
        max_iter = int(sys.argv[3])
    except:
        sys.exit("Invalid Arguments passed to Experiment")
else:
    cv = 5
    solver = "lbfgs"  # one of newton-cg, lbfgs, liblinear, sag, saga
    max_iter = 100

# Instantiate the model
clf = LogisticRegressionCV(cv=cv, solver=solver, max_iter=max_iter)
pipe = Pipeline([("ct", ct), ("scaler", StandardScaler()), ("clf", clf)])

# Train the model
pipe.fit(X_train, y_train)

# Capture train and test set scores
train_score = pipe.score(X_train, y_train)
test_score = pipe.score(X_test, y_test)
print("train", train_score)
print("test", test_score)
print(classification_report(y_test, pipe.predict(X_test)))
datadf[labels.name + " probability"] = pipe.predict_proba(X)[:, 1]


# Create LIME Explainer
feature_names = list(ce.columns_)
categorical_features = list(ce.cat_columns_ix_.values())
categorical_names = {i: ce.classes_[c] for c, i in ce.cat_columns_ix_.items()}
class_names = ["No " + labels.name, labels.name]
explainer = LimeTabularExplainer(
    ce.transform(datadf),
    feature_names=feature_names,
    class_names=class_names,
    categorical_features=categorical_features,
    categorical_names=categorical_names,
)


# Create and save the combined Logistic Regression and LIME Explained Model.
explainedmodel = ExplainedModel(
    data=datadf,
    labels=labels,
    categoricalencoder=ce,
    pipeline=pipe,
    explainer=explainer,
)
explainedmodel.save(model_name='telco_linear')


# If running as as experiment, this will track the metrics and add the model trained in this
# training run to the experiment history.
cdsw.track_metric("train_score", round(train_score, 2))
cdsw.track_metric("test_score", round(test_score, 2))
#cdsw.track_metric("model_path", explainedmodel.model_path)
#cdsw.track_file(explainedmodel.model_path)

# Wrap up

# We've now covered all the steps to **running Experiments**.
#
# Notice also that any script that will run as an Experiment can also be run as a Job or in a Session.
# Our provided script can be run with the same settings as for Experiments.
# A common use case is to **automate periodic model updates**.
# Jobs can be scheduled to run the same model training script once a week using the latest data.
# Another Job dependent on the first one can update the model parameters being used in production
# if model metrics are favorable.
