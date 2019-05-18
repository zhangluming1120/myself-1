'''
referenced from github 
link: https://github.com/bryangne/CMPT-318/tree/master/e3
'''
# coding: utf-8

# In[13]:

import sys
import numpy as np
import pandas as pd
import csv
from scipy import stats
# import matplotlib.pyplot as plt


# In[18]:

OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value: {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value: {more_searches_p:.3g}\n'
    '"Did more/less instructors use the search feature?" p-value: {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value: {more_instr_searches_p:.3g}'
)

def main() :
    filename = 'searches.json'
#     filename = sys.argv[1]
    search_df = pd.read_json(filename, orient='records',  lines=True)
    user_df = search_df[search_df['is_instructor'] == False]
    inst_df = search_df[search_df['is_instructor'] == True]
    #     | searched | unsearched
    # ---------------|------------
    # new |    x     |      x
    # old |    x     |      x
#     Number of (U)sers with (N)ew ui that have made (S)earches
    u_ns = search_df[(search_df['uid']%2==1) & (search_df['search_count']>0)]['uid'].count()
#     Number of (U)sers with (N)ew ui that are (U)nsearched
    u_nu = search_df[(search_df['uid']%2==1) & (search_df['search_count']==0)]['uid'].count()
#     Number of (U)sers with (O)ld ui that have made (S)earches
    u_os = search_df[(search_df['uid']%2==0) & (search_df['search_count']>0)]['uid'].count()
#     Number of (U)sers with (O)ld ui that are (U)nsearched
    u_ou = search_df[(search_df['uid']%2==0) & (search_df['search_count']==0)]['uid'].count()
    user_con = np.array([[u_ns, u_nu], 
                         [u_os, u_ou]])
    _, u_cp, _, _ = stats.chi2_contingency(user_con)
#     Get the manwhitney p value. Compare odd uid (new ui) user search counts with even uid search counts
    u_wp = stats.mannwhitneyu(search_df[search_df['uid']%2==1]['search_count'],
                              search_df[search_df['uid']%2==0]['search_count']).pvalue
    
#     Now, do the same for instructors
    i_ns = inst_df[(inst_df['uid']%2==1) & (inst_df['search_count']>0)]['uid'].count()
    i_nu = inst_df[(inst_df['uid']%2==1) & (inst_df['search_count']==0)]['uid'].count()
    i_os = inst_df[(inst_df['uid']%2==0) & (inst_df['search_count']>0)]['uid'].count()
    i_ou = inst_df[(inst_df['uid']%2==0) & (inst_df['search_count']==0)]['uid'].count()
    inst_con = np.array([[i_ns, i_nu], 
                         [i_os, i_ou]])
    _, i_cp, _, _ = stats.chi2_contingency(inst_con)
    i_wp = stats.mannwhitneyu(inst_df[inst_df['uid']%2==1]['search_count'],
                              inst_df[inst_df['uid']%2==0]['search_count']).pvalue
    
    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p=u_cp,
        more_searches_p=u_wp,
        more_instr_p=i_cp,
        more_instr_searches_p=i_wp,
    ))
    
if __name__ == '__main__':
    main()


# In[ ]:



