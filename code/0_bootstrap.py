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
    if os.path.exists("/etc/hadoop/conf/hive-site.xml"):
        tree = ET.parse("/etc/hadoop/conf/hive-site.xml")
        root = tree.getroot()
        for prop in root.findall("property"):
            if prop.find("name").text == "hive.metastore.warehouse.dir":
                storage = (
                    prop.find("value").text.split("/")[0]
                    + "//"
                    + prop.find("value").text.split("/")[2]
                )
    else:
        storage = "/user/" + os.getenv("HADOOP_USER_NAME")
    storage_environment_params = {"STORAGE": storage}
    storage_environment = cml.create_environment_variable(storage_environment_params)
    os.environ["STORAGE"] = storage

# define a function to run commands on HDFS
def run_cmd(cmd):

  """
  Run Linux commands using Python's subprocess module

  Args:
      cmd (str) - Linux command to run

  Returns:
      output
      errors
  """
  print("Running system command: {0}".format(cmd))

  args_list = cmd.split(" ")
  proc = subprocess.Popen(
      args_list, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
  )

  output, errors = proc.communicate()
  if proc.returncode != 0:
      raise RuntimeError(
          "Error running command: {}. Return code: {}, Error: {}".format(args_list, proc.returncode, errors)
      )
      
  return output, errors

# Attempt to upload the data to the cloud storage, if error,
# set environment variable indicating the use of local storage
# for project build
try:
    out, err = run_cmd(
        f'hdfs dfs -mkdir -p {os.environ["STORAGE"]}/{os.environ["DATA_LOCATION"]}'
    )
    out, err = run_cmd(
        f'hdfs dfs -copyFromLocal /home/cdsw/raw/WA_Fn-UseC_-Telco-Customer-Churn-.csv {os.environ["STORAGE"]}/{os.environ["DATA_LOCATION"]}/WA_Fn-UseC_-Telco-Customer-Churn-.csv'
    )
    cml.create_environment_variable({"STORAGE_MODE": "external"})
except RuntimeError as error:
    cml.create_environment_variable({"STORAGE_MODE": "local"})
    print(
        "Could not interact with external data store so local project storage will be used. HDFS DFS command failed with the following error:"
    )
    print(error)
