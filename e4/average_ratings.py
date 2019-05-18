
# coding: utf-8

# In[1]:


'''
referenced from github 
link: https://github.com/bryangne/CMPT-318/tree/master/e3
'''
import pandas as pd
import numpy as np
import csv
import sys
import difflib as dl


# In[7]:


# Given a text filename, read the text file line by line and place each line into a column called "names"
def readMovieNames(movie_list) :
    movie_df = pd.DataFrame(pd.read_csv(movie_list, sep="\n", header=None, names=['title']))
    return movie_df


# In[13]:


# Read a csv file containing movie-name/rating pairs
def readRatings(rating_list) :
    ratings_df = pd.read_csv(rating_list)
    return ratings_df


# In[14]:


# Input the movie name from the movie list, and the data frame containing the ratings.
# use difflib's get_close_matches to get a list of similar matches
# Take the resulting array and create a new pandas dataframe. Join this dataframe with the original ratings dataframe
# in to get only the ratings of matching titles. Take the mean of the entries and return the resulting dataframe
def getAverageScore(movie_name, rating_df) :
    matches = dl.get_close_matches(movie_name, rating_df['title'], n=250, cutoff=0.6)
#     df = df[df['Column Name'].isin(['Value']) == False]
    matches_df = rating_df.copy(deep=True)
    matches_df = matches_df[matches_df['title'].isin(matches)]
#     print(matches_df)
#     matches_df = pd.DataFrame(matches, columns=['title'])
#     matches_df = pd.merge(matches_df, rating_df, how='inner', on='title', left_index=True, right_index=False)
#     average = matches_df.sum(axis='rating')
    rating = matches_df['rating'].mean()
    # Round to two decimal places
    rating = "%.2f" % rating
#     print(rating)
    return (rating)


# In[15]:


# Apply the average score to each entry of the movie list
def findCloseMatches(movie_df, rating_df) :
    movie_df['average'] = movie_df.apply(lambda x : getAverageScore(x['title'], rating_df), axis=1)


# In[16]:


def main() :
#     movie_txt = sys.argv[1]
    movie_list = sys.argv[1]
    movie_df = readMovieNames(movie_list)
    rating_list = sys.argv[2]
    rating_df = pd.read_csv(rating_list)
#     getAverageScore('Iron Man 3', rating_df)
    findCloseMatches(movie_df, rating_df)
    print(movie_df)
    # Write the final dataframe to a csv file
    output_name = sys.argv[3]
    movie_df.to_csv(output_name)


# In[9]:


if __name__ == '__main__':
    main()

