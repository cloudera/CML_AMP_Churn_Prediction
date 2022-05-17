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

## Part 0: Bootstrap File
# You need to run this at the start of the project. It will install the requirements, create the
# STORAGE and STORAGE_MODE environment variables and copy the data from
# raw/WA_Fn-UseC_-Telco-Customer-Churn-.csv into specified path of the STORAGE
# location, if applicable.

# The STORAGE environment variable is the Cloud Storage location used by the DataLake
# to store hive data. On AWS it will be s3a://[something], on Azure it will be
# abfs://[something] and on a CDSW cluster, it will be hdfs://[something]

# Install the requirements
!pip3 install -r requirements.txt

# Create the directories and upload data
from cmlbootstrap import CMLBootstrap
from IPython.display import Javascript, HTML
import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime
import subprocess

run_time_suffix = datetime.datetime.now()
run_time_suffix = run_time_suffix.strftime("%d%m%Y%H%M%S")

# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY")
PROJECT_NAME = os.getenv("CDSW_PROJECT")

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# Set the STORAGE environment variable
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
    
  
# define a function to run commands on HDFS
def run_cmd(cmd, raise_err=True):

    """
    Run Linux commands using Python's subprocess module
    Args:
        cmd (str) - Linux command to run
    Returns:
        process
    """
    print("Running system command: {0}".format(cmd))

    proc = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

    if proc.returncode != 0 and raise_err == True:
        raise RuntimeError(
            "Error running command: {}. Return code: {}, Output: {}, Error: {}".format(
                cmd, proc.returncode, proc.stdout, proc.stderr
            )
        )

    return proc


# Attempt to upload the data to the cloud storage, if error,
# set environment variable indicating the use of local storage
# for project build
try:
    dataset_check = run_cmd(
        f'hdfs dfs -test -f {os.environ["STORAGE"]}/{os.environ["DATA_LOCATION"]}/WA_Fn-UseC_-Telco-Customer-Churn-.csv',
        raise_err=False,
    )

    if dataset_check.returncode != 0:
        run_cmd(
            f'hdfs dfs -mkdir -p {os.environ["STORAGE"]}/{os.environ["DATA_LOCATION"]}'
        )
        run_cmd(
            f'hdfs dfs -copyFromLocal /home/cdsw/raw/WA_Fn-UseC_-Telco-Customer-Churn-.csv {os.environ["STORAGE"]}/{os.environ["DATA_LOCATION"]}/WA_Fn-UseC_-Telco-Customer-Churn-.csv'
        )
    cml.create_environment_variable({"STORAGE_MODE": "external"})
except RuntimeError as error:
    cml.create_environment_variable({"STORAGE_MODE": "local"})
    print(
        "Could not interact with external data store so local project storage will be used. HDFS DFS command failed with the following error:"
    )
    print(error)

# Create the YAML file for tracking model lineage
# DOCS: https://docs.cloudera.com/machine-learning/cloud/model-governance/topics/ml-registering-lineage-for-model.html
yaml_text = f"""Churn Model API Endpoint:
        hive_table_qualified_names:                                             # this is a predefined key to link to training data
            - "{os.environ["HIVE_DATABASE"]}.{os.environ["HIVE_TABLE"]}@cm"     # the qualifiedName of the hive_table object representing                
        metadata:                                                               # this is a predefined key for additional metadata
            query: "select * from historical_data"                              # suggested use case: query used to extract training data
            training_file: "code/4_train_models.py"                             # suggested use case: training file used
    """

with open("lineage.yml", "w") as lineage:
    lineage.write(yaml_text)