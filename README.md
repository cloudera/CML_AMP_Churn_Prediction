# Churn Modeling with scikit-learn
This repository accompanies the [Visual Model Interpretability for Telco Churn](https://blog.cloudera.com/visual-model-interpretability-for-telco-churn-in-cloudera-data-science-workbench/) blog post and contains the code needed to build all project artifacts on CML. Additionally, this project serves as a working example of the concepts discussed in the Cloudera Fast Forward report on [Interpretability](https://ff06-2020.fastforwardlabs.com/) which is freely available for download.

![table_view](images/table_view.png)

The primary goal of this repo is to build a logistic regression classification model to predict the probability that a group of customers will churn from a fictitious telecommunications company. In addition, the model is interpreted using a technique called [Local Interpretable Model-agnostic Explanations (LIME)](https://github.com/marcotcr/lime). Both the logistic regression and LIME models are deployed using CML's real-time model deployment capability and exercised via a basic Flask-based web application that allows users to interact with the model to see which factors in the data have the most influence on the probability of a customer churning.

## Project Structure

The project is organized with the following folder structure:

```
.
├── code/              # Backend scripts, and notebooks needed to create project artifacts
├── flask/             # Assets needed to support the front end application
├── images/            # A collection of images referenced in project docs
├── models/            # Directory to hold trained models
├── raw/               # The raw data file used within the project
├── cdsw-build.sh      # Shell script used to build environment for experiments and models
├── model_metrics.db   # SQL lite database used to store model drift metrics
├── README.md
└── requirements.txt
```

By following the notebooks, scripts, and documentation in the `code` directory, you will understand how to perform similar classification tasks on CML, as well as how to use the platform's major features to your advantage. These features include: 

- Data ingestion and manipulation with Spark
- Streamlined model development and experimentation
- Point-and-click model deployment to a RESTful API endpoint
- Application hosting for deploying frontend ML applications
- Model operations including model governance and tracking of mode performance metrics

We will focus our attention on working within CML, using all it has to offer, while glossing over the details that are simply standard data science. We trust that you are familiar with typical data science workflows and do not need detailed explanations of the code.

If you have deployed this project as an Applied ML Prototype (AMP), you will not need to run any of the setup steps outlined [in this document](code/README.md) as everything is already installed for you. However, you may still find it instructive to review the documentation and their corresponding files, and in particular run through `code/2_data_exploration.ipynb` and `code/3_model_building.ipynb` in a Jupyter Notebook session to see the process that informed the creation of the final model. 

If you are building this project from source code without automatic execution of project setup, then you should follow the steps listed [in this document](code/README.md) carefully and in order.