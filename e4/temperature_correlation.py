
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import csv
import sys
import difflib as dl


# In[9]:


import os.path
os.path.exists('movie_list.txt')
myfile = open('movie_list.txt','r')
myfile.readlines()
for line in myfile:
    print (line)

