# Project Build Process

The following step-by-step instructions correspond to the project files in this directory and should be followed in sequential order.

### 0 Bootstrap

There are a couple of steps needed at the start to configure the Project and Workspace settings so each step will run successfully. If you are building the project from the source code, then you must run the project bootstrap file before running other steps.

Open the file `0_bootstrap.py` in a normal workbench Python3 session. You only need a 1 vCPU / 2 GiB instance. Once the session is loaded, click **Run > Run All Lines**. This will file will first install project requirements. Then it will create environment variables for the project called **STORAGE** which is the root of default file storage location for the Hive Metastore in the DataLake (e.g. `s3a://my-default-bucket` if on AWS), and **STORAGE_MODE** which indicates if external storage is available or not. If not, the project will be build using local project storage only. This script will also upload the data used in the project to `$STORAGE/$DATA_LOCATION/`. The original file comes as part of this git repo in the `raw` folder.


### 1 Ingest Data

This script will read in the data csv from the file uploaded to the object store (s3/adls) setup during the bootstrap and create a managed table in Hive. This is all done using Spark.

Open `1_data_ingest.py` in a Workbench session: Python3, 1 CPU, 2 GB. Run the file.


### 2 Explore Data

This Jupyter Notebook does some basic exploratory data analysis (EDA) and visualization. It is to show how EDA fits into the data science workflow.

![data](../images/data.png)

This time, open a Jupyter Notebook session (rather than a workbench session): Python3, 1 CPU, 2 GB and open the `2_data_exploration.ipynb` file. 

At the top of the page click **Cells > Run All**.


### 3 Model Building

This Jupyter Notebook demonstrates a walkthrough of the process for building the churn prediction model. It also shows more details on how the LIME model is created and a bit more on what LIME is actually doing.

Open a Jupyter Notebook session (rather than a workbench session): Python3, 1 CPU, 2 GB and open the `3_model_building.ipynb` file. 

At the top of the page click **Cells > Run All**.


### 4 Model Training

A pre-trained model saved with the repo has been placed in the `models` directory. If you want to retrain the model, open the `4_train_models.py` file in a workbench session: Python3 1 vCPU, 2 GiB and run the file. The newly trained model will be saved in the models directory named `telco_linear.pkl`. 

There are 2 other ways of running the model training process:

***1. Jobs***

The **[Jobs](https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-job.html)** feature in CML allows for adhoc, recurring, and dependency triggered jobs to run specific scripts. To run this model training process as a job, create a new job by going to the project window and clicking _Jobs > New Job_ (in the left side bar) and entering the following settings:

* **Name** : Train Model
* **Script** : 4_train_models.py
* **Arguments** : _Leave blank_
* **Kernel** : Python 3
* **Schedule** : Manual
* **Engine Profile** : 1 vCPU / 2 GiB

The rest can be left as is. Once the job has been created, click **Run** to start a manual run for that job.

***2. Experiments***

The other option is running an **[Experiment](https://docs.cloudera.com/machine-learning/cloud/experiments/topics/ml-running-an-experiment.html)**. Experiments run immediately and are used for testing different parameters in a model training process. In this instance, Experiments would be used for hyperparameter optimization. To run an experiment, from the project window click *Experiments > Run Experiment* with the following settings:

* **Script** : 4_train_models.py
* **Arguments** : 5 lbfgs 100 (these the cv, solver and max_iter parameters to be passed to LogisticRegressionCV function)
* **Kernel** : Python 3
* **Engine Profile** : 1 vCPU / 2 GiB

Click **Start Run** and the experiment will be scheduled to build and run. Once the Run has completed, you can view the outputs that are tracked with the experiment using the `cdsw.track_metrics` function. It's worth reading through the code to get a sense of what all is going on.


### 5 Serve Model

The **[Models](https://docs.cloudera.com/machine-learning/cloud/models/topics/ml-creating-and-deploying-a-model.html)** feature in CML is used top deploy a machine learning model into production for real-time prediction. To deploy the model that was trained in the previous step, navigate to the project page, then click *Models > New Model* and create a new model with the following details:

* **Name**: Churn Model API Endpoint
* **Description**: Explain customer churn prediction
* **File**: 5_model_serve_explainer.py
* **Function**: explain
* **Input**: 

```
{
	"StreamingTV": "No",
	"MonthlyCharges": 70.35,
	"PhoneService": "No",
	"PaperlessBilling": "No",
	"Partner": "No",
	"OnlineBackup": "No",
	"gender": "Female",
	"Contract": "Month-to-month",
	"TotalCharges": 1397.475,
	"StreamingMovies": "No",
	"DeviceProtection": "No",
	"PaymentMethod": "Bank transfer (automatic)",
	"tenure": 29,
	"Dependents": "No",
	"OnlineSecurity": "No",
	"MultipleLines": "No",
	"InternetService": "DSL",
	"SeniorCitizen": "No",
	"TechSupport": "No"
}
```

* **Kernel**: Python 3
* **Engine Profile**: 1vCPU / 2 GiB Memory

Leave the rest unchanged. Click **Deploy Model** and the model will go through the build process and deploy a REST endpoint. Once the model is deployed, you can test it is working from the model *Model Overview* page.

**Note:** Once the model is deployed, you must disable the additional model authentication feature. In the model settings page, untick **Enable Authentication**.

![disable_auth](../images/disable_auth.png)

### 6 Deploy Application

The next step is to deploy the Flask application. The **[Applications](https://docs.cloudera.com/machine-learning/cloud/applications/topics/ml-applications.html)** feature is still quite new for CML. For this project it is used to deploy a web based application that interacts with the underlying model created in the previous step.

**Note:** In the deployed model from Step 5, go to **Model > Settings** and make a note (i.e. copy) the 
"Access Key". It will look something like this (ie. mukd9sit7tacnfq2phhn3whc4unq1f38)

From the project level, click on "Open Workbench" (note you don't actually have to Launch a 
session) in order to edit a file. Select the flask/single_view.html file and paste the Access 
Key in at line 19.

`        const accessKey = <your_access_key_here>;`

Save the file (if it has not auto saved already) and go back to the Project. Go to the **Applications** section and select "New Application" with the following details

* **Name**: Churn Analysis App
* **Subdomain**: churn-app _(note: this needs to be unique, so if you've done this before, 
  pick a more random subdomain name)_
* **Script**: 6_application.py
* **Kernel**: Python 3
* **Engine Profile**: 1vCPU / 2 GiB Memory

After the application deploys, click on the blue-arrow next to the name. The initial view is a table of randomly selected records from the dataset. This shows a global view of which features are most important for the predictor model. The red shows increased importance for predicting a customer will churn and the blue for customers that will not churn.

![table_view](../images/table_view.png)

Clicking on any single row will show a "local" interpreted model for that particular data point instance. Here you can see how adjusting any one of the features will change the instance's churn prediction.


![single_view_1](../images/single_view_1.png)

Changing the InternetService to DSL lowers the probability of churn. This does not mean that changing the Internet Service to DSL cause the probability to go down, this is just what the model would predict for a customer with those data points.


![single_view_2](../images/single_view_2.png)

### 7 Model Operations

The final step is the model operations which consists of [Model Metrics](https://docs.cloudera.com/machine-learning/cloud/model-metrics/topics/ml-enabling-model-metrics.html) and [Model Governance](https://docs.cloudera.com/machine-learning/cloud/model-governance/topics/ml-enabling-model-governance.html).

**Model Governance** is setup in the `0_bootstrap.py` script, which writes out the `lineage.yml` file at the start of the project to provide an integration point with Apache Atlas. For **Model Metrics**, open a workbench session (1 vCPU / 2 GiB) and open the `7a_ml_ops_simulation.py` file. You need to set the `model_id` number from the model created in step 5 on line 113. The model number is also located on the model's main page:

![model_id](../images/model_id.png)

`model_id = "95"`

From there, run the file. This goes through a process of simulating a model that drifts over over 1000 calls to the model. The file contains comments with details of how this is done.

In the this step, you can interact and display the model metrics. Open a workbench session (1 vCPU / 2 GiB). Then open and run the `7b_ml_ops_visual.py` file. Again you will need to set the `model_id` number from the model created in step 5 on line 53. The model number is on the model's main page.

![model_accuracy](../images/model_accuracy.png)