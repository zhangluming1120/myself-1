
# coding: utf-8

# In[277]:


import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt


# In[278]:




# In[279]:


filename1 = 'pagecounts-20160802-150000.txt'
filename2 = 'pagecounts-20160803-150000.txt'


# In[280]:


raw_data1 = pd.read_table(filename1, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes']).reset_index()
raw_data1


# In[281]:


raw_data1.sort_values('views', axis=0, ascending=False, inplace=True)
raw_data1


# In[282]:


raw_data2 = pd.read_table(filename2, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes']).reset_index()


# In[283]:


raw_data2.sort_values('views', axis=0, ascending=False, inplace=True)
raw_data2


# In[284]:


import matplotlib.pyplot as plt


# In[285]:


plt.figure(figsize=(10, 5)) # change the size to something sensible
plt.subplot(1, 2, 1) # subplots in 1 row, 2 columns, select the first
plt.plot(raw_data1['views'].values) # build plot 1
plt.title('Population Distribution')
plt.xlabel('Ranks')
plt.ylabel('Views')
plt.savefig('day1.png')


# In[286]:


raw_data1.sort_values('views', axis=0, ascending=True, inplace=True)
raw_data1


# In[287]:


raw_data2.sort_values('views', axis=0, ascending=True, inplace=True)
raw_data2


# In[288]:


merged = pd.merge(raw_data1, raw_data2, how='inner', on=['page'])
merged.plot(x='views_x', y='views_y', style='.')
plt.xscale('log')
plt.yscale('log')
plt.title('Daily Correlation')
plt.xlabel('Day 1 views')
plt.ylabel('Day 2 views')
plt.savefig('comparison.png')

