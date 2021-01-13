# Churn Modeling with scikit-learn
This repository accompanies the [Visual Model Interpretability for Telco Churn](https://blog.cloudera.com/visual-model-interpretability-for-telco-churn-in-cloudera-data-science-workbench/) blog post and contains the code to build all project artifacts on CML. Additionally, this project serves as a working example of the concepts discussed in the Cloudera Fast Forward report on [Interpretability](https://ff06-2020.fastforwardlabs.com/) which is freely available for download.

![table_view](images/table_view.png)

The primary goal of this repo is to build a logistic regression classification model to predict the probability that a group of customers will churn from a fictitious telecommunications company. On top that, the model is interpreted using a technique called [Local Interpretable Model-agnostic Explanations (LIME)](https://github.com/marcotcr/lime). Both the logistic regression and LIME models are then deployed using CML's real-time model deployment capability and finally are served to a basic Flask-based web application that allows users to interact with the real-time model to see which factors in the data have the most influence on the probability of a customer churning.

## Project Structure

The project is organized with the following folder structure:

```
.
├── README.md
├── cdsw-build.sh      # Shell script used to build environment for experiments and models
├── code               # Backend scripts, and notebooks needed to create project artifacts
├── flask              # Assets needed to support the front end application
├── images             # A collection of images referenced in project docs
├── lineage.yml        # Configures Apache Atlas integration for model governance
├── model_metrics.db
├── models             # Directory to hold trained models
├── raw                # The raw data file used within the project
└── requirements.txt
```

By following the notebooks and scripts in the `code` folder, you will understand how to perform similar classification tasks on CML, as well as how to use the platform's major features to your advantage. These features include *data ingestion with Spark*, *streamlined model experimentation*, *point-and-click model deployment*, and *ML application hosting*. We will focus our attention on working within CML, using all it has to offer, while glossing over the details that are simply standard data science. We trust that you are familiar with typical data science workflows and do not need detailed explanations of the code.

If you have deployed this project as an Applied ML Prototype (AMP), you will not need to run any of the setup steps outlined [in this document](code/README.md) as everything is already installed for you. However, you may still find it instructive to review the documentation and their corresponding files, and in particular run through `code/2_data_exploration.ipynb` and `code/3_model_building.ipynb` in a Jupyter Notebook session to see the process that informed the creation of the final model. 

If you are building this project from source code without automatic execution of project setup, then you should follow the steps listed [in this document](code/README.md) carefully and in order.