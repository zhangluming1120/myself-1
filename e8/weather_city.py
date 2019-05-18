
# coding: utf-8

# In[9]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from skimage.color import lab2rgb
import sys

from sklearn.naive_bayes import GaussianNB
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import make_pipeline
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier

from sklearn.model_selection import train_test_split

from sklearn.preprocessing import StandardScaler


# In[45]:


OUTPUT_TEMPLATE = (
    'Bayesian classifier: {bayes:.3g}\n'
    'kNN classifier:      {knn:.3g}\n'
    'SVM classifier:      {svm:.3g}\n'
)


#filename1 = 'monthly-data-labelled.csv'
#filename2 = 'monthly-data-unlabelled.csv'
#filename3 = 'sample-labels.csv'
filename1 = sys.argv[1]
filename2 = sys.argv[2]
filename3 = sys.argv[3]

labeled = pd.read_csv(filename1)
unlabeled = pd.read_csv(filename2)
labeled = labeled.drop (['year'], axis=1)
X = labeled.drop(['city'], axis = 1).values
y = labeled['city'].values
X_train, X_test, y_train, y_test = train_test_split(X, y)
bayes_model = GaussianNB()
knn_model = KNeighborsClassifier(n_neighbors=5)
svc_model = SVC(kernel='linear', decision_function_shape='ovr')
models = [bayes_model, knn_model, svc_model]
for i, m in enumerate(models):  # yes, you can leave this loop in if you want.
        m.fit(X_train, y_train)
    
print(OUTPUT_TEMPLATE.format(
        bayes = bayes_model.score(X_test, y_test),
        knn=knn_model.score(X_test, y_test),
        svm=svc_model.score(X_test, y_test),
    ))
#svc_model.predict()
X_ = unlabeled.drop(['city', 'year'], axis = 1).values

predictions = svc_model.predict(X_)
pd.Series(predictions).to_csv(filename3, index=False)



