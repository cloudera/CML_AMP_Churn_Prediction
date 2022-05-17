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

import datetime, dill, os
import numpy as np
import pandas as pd

from sklearn.pipeline import TransformerMixin
from sklearn.preprocessing import LabelEncoder


"""
Explained model is a class that has attributes:

 - data, i.e. the features you get for a given dataset from load_dataset. This
   is a pandas dataframe that may include categorical variables.
 - labels, i.e. the boolean labels you get for a given dataset from
   load_dataset.
 - categoricalencoder, a fitted sklearn Transformer object that transforms
   the categorical columns in `data` to deterministic integer codes, yielding a
   plain numpy array often called `X` (leaves non-categorical columns
   untouched)
 - pipeline, a trained sklearn pipeline that takes `X` as input and predicts.
 - explainer, an instantiated LIME explainer that yields an explanation when
   it's explain instance method is run on an example `X`

properties:
 - default_data
 - categorical_features
 - non_categorical_features
 - dtypes

and methods for API (which works in terms of dictionaries):
 - cast_dct, converts values of dictionary to dtype corresponding to key
 - explain_dct, returns prediction and explanation for example dictionary

and methods for users (who usually have dataframes):
 - predict_df, returns predictions for a df, i.e. runs it through categorical
   encoder and pipeline
 - explain_df, returns predictions and explanation for example dataframe
"""

DATA_DIR = "/home/cdsw"


class ExplainedModel:
    def __init__(
        self,
        labels=None,
        data=None,
        categoricalencoder=None,
        pipeline=None,
        explainer=None,
    ):

        self.data = data
        self.labels = labels
        self.categoricalencoder = categoricalencoder
        self.pipeline = pipeline
        self.explainer = explainer

    @staticmethod
    def load(model_name) -> "ExplainedModel":
        """Returns an ExplainedModel object"""
        model_dir = os.path.join(DATA_DIR, "models", model_name)
        model_path = os.path.join(model_dir, model_name + ".pkl")
        result = ExplainedModel()
        try:
            with open(model_path, "rb") as f:
                result.__dict__.update(dill.load(f))
            return result
        except OSError as err: 
            print(f"Model path does not exist, returned error: {err}")

    def save(self, model_name):
        model_dir = os.path.join(DATA_DIR, "models", model_name)
        # ensure directory exists
        os.makedirs(model_dir, exist_ok=True)
        model_path = os.path.join(model_dir, model_name + ".pkl")
        dilldict = {
            "data": self.data,
            "labels": self.labels,
            "categoricalencoder": self.categoricalencoder,
            "pipeline": self.pipeline,
            "explainer": self.explainer,
        }
        with open(model_path, "wb") as f:
            dill.dump(dilldict, f)

    def predict_df(self, df):
        X = self.categoricalencoder.transform(df)
        return self.pipeline.predict_proba(X)[:, 1]

    def explain_df(self, df):
        X = self.categoricalencoder.transform(df)
        probability = self.pipeline.predict_proba(X)[0, 1]
        e = self.explainer.explain_instance(X[0], self.pipeline.predict_proba).as_map()[
            1
        ]
        explanations = {self.explainer.feature_names[c]: weight for c, weight in e}
        return probability, explanations

    def explain_dct(self, dct):
        return self.explain_df(pd.DataFrame([dct]))

    def cast_dct(self, dct):
        dct = {k: self.dtypes[k].type(v) for k, v in dct.items()}
        dct = {
            k: (v if type(v) != np.dtype("int64") else int(v)) for k, v in dct.items()
        }
        return dct

    @property
    def dtypes(self):
        if not hasattr(self, "_dtypes"):
            d = self.data[self.non_categorical_features].dtypes.to_dict()
            d.update(
                {
                    c: self.data[c].cat.categories.dtype
                    for c in self.categorical_features
                }
            )
            self._dtypes = d
        return self._dtypes

    @property
    def non_categorical_features(self):
        return list(
            self.data.select_dtypes(exclude=["category"]).columns.drop(
                self.labels.name + " probability"
            )
        )

    @property
    def categorical_features(self):
        return list(self.data.select_dtypes(include=["category"]).columns)

    @property
    def stats(self):
        def describe(s):
            return {
                "median": s.median(),
                "mean": s.mean(),
                "min": s.min(),
                "max": s.max(),
                "std": s.std(),
            }

        if not hasattr(self, "_stats"):
            self._stats = {
                c: describe(self.data[c]) for c in self.non_categorical_features
            }
        return self._stats

    @property
    def label_name(self):
        return self.labels.name + " probability"

    @property
    def categories(self):
        return {
            feature: list(self.categoricalencoder.classes_[feature])
            for feature in self.categorical_features
        }

    @property
    def default_data(self):
        # 0th class for categorical variables and mean for continuous
        if not hasattr(self, "_default_data"):
            d = {}
            d.update(
                {
                    feature: self.categoricalencoder.classes_[feature][0]
                    for feature in self.categorical_features
                }
            )
            d.update(
                {
                    feature: self.data[feature].median()
                    for feature in self.non_categorical_features
                }
            )
            self._default_data = d
        return self._default_data


class CategoricalEncoder(TransformerMixin):
    def fit(self, X, y=None, *args, **kwargs):
        self.columns_ = X.columns
        self.cat_columns_ix_ = {
            c: i
            for i, c in enumerate(X.columns)
            if pd.api.types.is_categorical_dtype(X[c])
        }
        self.cat_columns_ = pd.Index(self.cat_columns_ix_.keys())
        self.non_cat_columns_ = X.columns.drop(self.cat_columns_)
        self.les_ = {c: LabelEncoder().fit(X[c]) for c in self.cat_columns_}
        self.classes_ = {c: list(self.les_[c].classes_) for c in self.cat_columns_}
        return self

    def transform(self, X, y=None, *args, **kwargs):
        data = X[self.columns_].values
        for c, i in self.cat_columns_ix_.items():
            data[:, i] = self.les_[c].transform(data[:, i])
        return data.astype(float)

    def __repr__(self):
        return "{}()".format(self.__class__.__name__)
