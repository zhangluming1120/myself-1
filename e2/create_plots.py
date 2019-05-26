

import numpy as np
import pandas as pd
import sys
import matplotlib.pyplot as plt

filename1 = 'pagecounts-20190509-120000.txt'
filename2 = 'pagecounts-20190509-130000.txt'

raw_data1 = pd.read_csv(filename1, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes']).reset_index()

raw_data1.sort_values('views', axis=0, ascending=False, inplace=True)


raw_data2 = pd.read_table(filename2, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes']).reset_index()

raw_data2.sort_values('views', axis=0, ascending=False, inplace=True)


plt.figure(figsize=(10, 5)) # change the size to something sensible
plt.subplot(1, 2, 1) # subplots in 1 row, 2 columns, select the first
plt.plot(raw_data1['views'].values) # build plot 1
plt.title('Population Distribution')
plt.xlabel('Ranks')
plt.ylabel('Views')

merged = pd.merge(raw_data1, raw_data2, how='inner', on=['page'])
plt.subplot(1, 2, 2) # subplots in 1 row, 2 columns, select the first
plt.plot(merged['views_x'], merged['views_y'], '.') # build plot 1
plt.xscale('log')
plt.yscale('log')
plt.title('Daily Correlation')
plt.xlabel('Day 1 views')
plt.ylabel('Day 2 views')
plt.savefig('comparison.png')
