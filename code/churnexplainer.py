import datetime, dill, os
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

class ExplainedModel():

    def __init__(self, model_name=None, labels=None, data=None, #dataset=None, data=None, labels=None,
                 categoricalencoder=None, pipeline=None, explainer=None, data_dir=None,
                 load=True):
        if model_name is not None:
          self.model_name = model_name
          self.is_loaded = False
        else:
          self.data = data
          self.labels = labels
          self.categoricalencoder = categoricalencoder
          self.pipeline = pipeline
          self.explainer = explainer
          self.is_loaded = True
        self.model_dir = os.path.join(data_dir, 'models', self.model_name)
        self.model_path = os.path.join(self.model_dir,
                                       self.model_name + '.pkl')
        # if asked to load and not yet loaded, load model!
        if load and not self.is_loaded:
            self.load()

    def load(self):
        if not self.is_loaded:
            with open(self.model_path, 'rb') as f:
                self.__dict__.update(dill.load(f))
            self.is_loaded = True

    def save(self):
        dilldict = {
            'data': self.data,
            'labels': self.labels,
            'categoricalencoder': self.categoricalencoder,
            'pipeline': self.pipeline,
            'explainer': self.explainer
        }
        #self._make_model_dir()
        with open(self.model_path, 'wb') as f:
            dill.dump(dilldict, f)

#    def _make_model_name(self):
#        now = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
#        model_type = os.environ.get('CHURN_MODEL_TYPE', 'linear')
#        #model_name = '_'.join([now, self.dataset, model_type, get_git_hash()])
#        model_name = '_'.join([now, self.dataset, model_type])
#        return model_name
#
#    def _make_model_dir(self):
#        if not os.path.exists(self.model_dir):
#            os.makedirs(self.model_dir)

    def predict_df(self, df):
        X = self.categoricalencoder.transform(df)
        return self.pipeline.predict_proba(X)[:, 1]

    def explain_df(self, df):
        X = self.categoricalencoder.transform(df)
        probability = self.pipeline.predict_proba(X)[0, 1]
        e = self.explainer.explain_instance(
            X[0], self.pipeline.predict_proba
        ).as_map()[1]
        explanations = {self.explainer.feature_names[c]: weight
                        for c, weight in e}
        return probability, explanations

    def explain_dct(self, dct):
        return self.explain_df(pd.DataFrame([dct]))

    def cast_dct(self, dct):
        return {k: self.dtypes[k].type(v) for k, v in dct.items()}

    @property
    def dtypes(self):
        if not hasattr(self, '_dtypes'):
            d = self.data[self.non_categorical_features].dtypes.to_dict()
            d.update({c: self.data[c].cat.categories.dtype
                      for c in self.categorical_features})
            self._dtypes = d
        return self._dtypes

    @property
    def non_categorical_features(self):
        return list(self.data.select_dtypes(exclude=['category']).columns
                    .drop(self.labels.name + ' probability'))

    @property
    def categorical_features(self):
        return list(self.data.select_dtypes(include=['category']).columns)

    @property
    def stats(self):
        def describe(s):
            return {'median': s.median(),
                    'mean': s.mean(),
                    'min': s.min(),
                    'max': s.max(),
                    'std': s.std()}
        if not hasattr(self, '_stats'):
            self._stats = {c: describe(self.data[c])
                           for c in self.non_categorical_features}
        return self._stats

    @property
    def label_name(self):
        return self.labels.name + ' probability'

    @property
    def categories(self):
        return {feature: list(self.categoricalencoder.classes_[feature])
                for feature in self.categorical_features}

    @property
    def default_data(self):
        # 0th class for categorical variables and mean for continuous
        if not hasattr(self, '_default_data'):
            d = {}
            d.update({feature: self.categoricalencoder.classes_[feature][0]
                      for feature in self.categorical_features})
            d.update({feature: self.data[feature].median()
                      for feature in self.non_categorical_features})
            self._default_data = d
        return self._default_data

class CategoricalEncoder(TransformerMixin):

    def fit(self, X, y=None, *args, **kwargs):
        self.columns_ = X.columns
        self.cat_columns_ix_ = {c: i for i, c in enumerate(X.columns)
                                if pd.api.types.is_categorical_dtype(X[c])}
        self.cat_columns_ = pd.Index(self.cat_columns_ix_.keys())
        self.non_cat_columns_ = X.columns.drop(self.cat_columns_)
        self.les_ = {c: LabelEncoder().fit(X[c])
                     for c in self.cat_columns_}
        self.classes_ = {c: list(self.les_[c].classes_)
                         for c in self.cat_columns_}
        return self

    def transform(self, X, y=None, *args, **kwargs):
        data = X[self.columns_].values
        for c, i in self.cat_columns_ix_.items():
            data[:, i] = self.les_[c].transform(data[:, i])
        return data.astype(float)

    def __repr__(self):
        return('{}()'.format(self.__class__.__name__))
