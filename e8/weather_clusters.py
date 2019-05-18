
# coding: utf-8

# In[21]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sys

from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import MinMaxScaler

from sklearn.decomposition import PCA
from sklearn.cluster import KMeans


# In[17]:


def get_pca(X):
    """
    Transform data to 2D points for plotting. Should return an array with shape (n, 2).
    """
    flatten_model = make_pipeline(
        PCA(2),
        MinMaxScaler(copy=True, feature_range=(0, 1))
    )
    X2 = flatten_model.fit_transform(X)
    assert X2.shape == (X.shape[0], 2)
    return X2


# In[22]:




def get_clusters(X):
    """
    Find clusters of the weather data.
    """
    model = make_pipeline(
        # TODO
        MinMaxScaler(copy=True, feature_range=(0, 1))
        KMeans(n_clusters=10, random_state=0)
    )
    model.fit(X)
    return model.predict(X)


# In[23]:


#filename1 = 'monthly-data-labelled.csv'
filename1 = sys.argv[1]
labeled = pd.read_csv(filename1)
labeled = labeled.drop (['year'], axis=1)
X = labeled.drop(['city'], axis = 1).values
y = labeled['city'].values
X2 = get_pca(X)



# In[25]:


clusters = get_clusters(X)
plt.scatter(X2[:, 0], X2[:, 1], c=clusters, cmap='Set1', edgecolor='k', s=20)
plt.savefig('clusters.png')

df = pd.DataFrame({
    'cluster': clusters,
    'city': y,
})
counts = pd.crosstab(df['city'], df['cluster'])
print(counts)

