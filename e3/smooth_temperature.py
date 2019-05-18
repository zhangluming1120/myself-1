
# coding: utf-8

# In[178]:
'''
referenced from github
resourse currently unavailable
'''
import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess
from pykalman import KalmanFilter


# In[180]:


def RunLowessFilter(cpu_data) :
    filtered = lowess(cpu_data['temperature'], cpu_data['timestamp'], frac=0.05)
    return filtered


# In[181]:


def RunKalmanFilter(cpu_data) :
    kalman_data = cpu_data[['temperature', 'cpu_percent']]
    # iloc gets the specified row of data
    initial_state = kalman_data.iloc[0]
    observation_stddev = 0.9
    transition_stddev = 0.3
    observation_covariance = [[observation_stddev ** 2, 0], [0, 2 ** 2]]
    transition_covariance = [[transition_stddev ** 2, 0], [0, 10 ** 2]]
    transition_matrix = [[1, 0.125], [0, 1]]
    kf = KalmanFilter(
        initial_state_mean=initial_state,
        initial_state_covariance=observation_covariance,
        observation_covariance=observation_covariance,
        transition_covariance=transition_covariance,
        transition_matrices=transition_matrix
    )
    kalman_smoothed, _ = kf.smooth(kalman_data)
    return kalman_smoothed


# In[182]:


cpu_data = pd.read_csv('sysinfo.csv', parse_dates=['timestamp'])
lowess_smoothed = lowess(cpu_data['temperature'], cpu_data['timestamp'], frac=0.05)
#kalman_smoothed = RunKalmanFilter(cpu_data)
plt.figure(figsize=(12, 4))
#plt.plot(cpu_data['timestamp'], kalman_smoothed[:, 0], 'g-')
plt.plot(cpu_data['timestamp'], lowess_smoothed[:, 1], 'r-', linewidth=1)
plt.plot(cpu_data['timestamp'], cpu_data['temperature'], '.', alpha=0.5)
plt.savefig('cpu.svg') # for final submission

