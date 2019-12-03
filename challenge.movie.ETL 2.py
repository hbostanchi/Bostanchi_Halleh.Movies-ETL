#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import pandas as pd
import numpy as np
import os
import re
from sqlalchemy import create_engine
import psycopg2
from config import db_password
import time


# In[2]:


#load wikipedia datas in JASON file list of dictionaries
#file_dir = '.'
file_dir = '/Users/halleh/Desktop/Resources/Bostanchi_Halleh.Movies-ETL'



def file_ETL(wiki, kaggle, ratings):
   #Call new_files_to_load function
    with open('{file_dir}/wikipedia-movies.json', mode='r') as file:
        wiki_movies_raw = json.load(file)
    kaggle_metadata = pd.read_csv(f'{file_dir}/{kaggle_file}', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}/{ratings}', low_memory=False)
    print('The Extract step is done.')
        # Create a list comprehension with the filter expression
    wiki_movies = [movie for movie in wiki_movies_raw if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
    wiki_movies_df=pd.DataFrame(wiki_movies_raw)
print('wiki_movies is created.') 
     



# In[4]:


#wiki_movies_df=pd.DataFrame(wiki_movies_raw)


# In[13]:


def clean_movie(movie):
    movie = dict(movie) #create a non-destructive copy
    alt_titles = {}
    # combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles

    # merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')

    return movie
   #Create a list of clean movies and create a dataframe
    clean_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(clean_movies)
    print('wiki_movies_df is cleaned.')
        
    #Get IMDb IDs from IMDb links and drop  duplicates
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
    
    #Find and keep columns with less than 90% null values and update dataframe
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
    print('wiki_movies_df is updated.')   
        
    #Drop rows with missing values in columns
    box_office = wiki_movies_df['Box office'].dropna()
    budget = wiki_movies_df['Budget'].dropna()
        
    #Concatenate all list items into one string
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    #Create a variable form_one and set it equal to the finished regular expression string
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    #Create another variable form_two and set it equal to the finished regular expression string
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        
    #date forms
    #1-Full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    #2-Four-digit year, two-digit month, two-digit day, with any separator (i.e., 2000-01-01)
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    #3-Full month name, four-digit year (i.e., January 2000)
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    #4-Four-digit year
    date_form_four = r'\d{4}'
        
    #Search for any string that starts with a dollar sign and ends with a hyphen, and then replace it with just a dollar sign
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    #Convert strings to numeric values.
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        


# In[50]:


def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
      s = re.sub('\$|,','', s)

        # convert to float
    value = float(s)

        # return value
    return value

# otherwise, return NaN
    else:
            return np.nan
    budget = wiki_movies_df['Budget'].dropna()
             # Convert any lists to strings:
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    matches_form_one = budget.str.contains(form_one, flags=re.IGNORECASE)
    matches_form_two = budget.str.contains(form_two, flags=re.IGNORECASE)
    budget[~matches_form_one & ~matches_form_two]
    budget = budget.str.replace(r'\[\d+\]\s*', '')
    budget[~matches_form_one & ~matches_form_two]
    #Extract the values from box_office using str.extract. 
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    #Drop the Box Office column.
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    # Make the release_date variable.
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    #Make the release_date variable.
    release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    #Convert strings to numeric values.
    running_time_extract = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
       
    #Drop the old Box Office column
    wiki_movies_df.drop('Box office', axis=1, inplace=True)
    wiki_movies_df.drop('Budget', axis=1, inplace=True)
    wiki_movies_df.drop('Release date', axis=1, inplace=True)
    wiki_movies_df.drop('Running time', axis=1, inplace=True)
    # Remove the bad data.
    kaggle_metadata[~kaggle_metadata['adult'].isin(['True','False'])]
    # Keep rows where the adult column is False, and then drop the adult column.
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
    #Covert to numeric columns
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        
    #Convert release_date to datetime
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
        
    #Convert timestamp to datetime
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
        
        #Identify columns that are redundant across two datasets
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
        
    #Drop rows where data is corrupted because two movies got merged
    try:
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
    except:
        print('No rows to drop because of data corruption')
            
    #Drop the title_wiki, release_date_wiki, Language, and Production company(s) columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    #Create a function that fills in missing data for a column pair and then drops the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)
          
    #Run the function for the three column pairs that we decided to fill in zeros
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
        
    #Check that there aren’t any columns with only one value.
    try:
        for col in movies_df.columns:
            lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
            value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
            num_values = len(value_counts)
            if num_values == 1:
                #drop the column as we don't need it
                movies_df.drop(col, axis=1, inplace=True)
    except:
        print('No columns to drop because of only one value')
        
    #Reorder the columns
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
        
    #Rename the columns
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    print('step3 is done')     
        
    #Step4: Transform and merge ratings data
        
    #Count how many times a movie received a given rating:
    #Use a groupby on the “movieId” and “rating” columns and take the count for each group
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()

    #Rename the “userId” column to “count.”
    rating_counts = rating_counts.rename({'userId':'count'}, axis=1) 

    #Pivot this data so that movieId is the index, the columns will be all the rating values, and the rows will be the counts for each rating value.
    rating_counts = rating_counts.pivot(index='movieId',columns='rating', values='count')

    #Rename the columns so they’re easier to understand.
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
        
    #Merge the rating counts in movies_df:
    #Use a left merge, since we want to keep everything in movies_df.
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

    #Fill in missing values
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    print('step4 is done')    
    print('Files have been transformed successfully.')
        
    #Step5: Load and update data into SQL tables
        
    #Establish connection string:
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

    #Create the database engine
    engine = create_engine(db_string)
        
    #Delete data from movies table but keep tables
    try:
        connection = psycopg2.connect(db_string)
        cursor = connection.cursor()
        sql_delete_query = "DELETE FROM movies"
        cursor.execute(sql_delete_query)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record deleted successfully from movies")

    except (Exception, psycopg2.Error) as error:
        print("Record from movies deleted. Complete.")
    
    print("COMPLETE. Record deleted successfully from movies")    
    
    #Delete data from ratings table but keep tables
    try:
        connection = psycopg2.connect(db_string)
        cursor = connection.cursor()
        sql_delete_query = "DELETE FROM ratings"
        cursor.execute(sql_delete_query)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record deleted successfully from ratings")

    except (Exception, psycopg2.Error) as error:
        print("Error in Delete operation", error)
            
    print("COMPLETE. Record deleted successfully from ratings")   
    
    # Fill in missing values for movies that did not get a rating
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    print('The Transform step is done.')

    # Create a database engine
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)

    # Import the DataFrame to a SQL table
    movies_df.to_sql(name='movies_challenge', con=engine, if_exists='append')
    print('The table movies_challenge is done.')
        
    #Import the ratings data
    # create a variable for the number of rows imported
    rows_imported = 0
    # get the start_time from time.time()
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
        # print out the range of rows that are being imported
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            
        data.to_sql(name='ratings', con=engine, if_exists='append')

        # increment the number of rows imported by the size of 'data'
        rows_imported += len(data)

        # add elapsed time to final print out
        print(f'Done. {time.time() - start_time} total seconds elapsed')


# In[52]:


#Identify columns that are redundant across two datasets
movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
        
    #Drop rows where data is corrupted because two movies got merged
try:
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
except:
        print('No rows to drop because of data corruption')
            
    #Drop the title_wiki, release_date_wiki, Language, and Production company(s) columns
movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    #Create a function that fills in missing data for a column pair and then drops the redundant column
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
            lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)
          
    #3-Run the function for the three column pairs that we decided to fill in zeros
fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
        
    #Check that there aren’t any columns with only one value.
try:
        for col in movies_df.columns:
            lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
            value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
            num_values = len(value_counts)
            if num_values == 1:
                #drop the column as we don't need it
                movies_df.drop(col, axis=1, inplace=True)
except:
        print('No columns to drop because of only one value')
        
    #Reorder the columns
movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]
        
    #Rename the columns
movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)
    
        
    # Transform and merge ratings data
        
    #Count how many times a movie received a given rating:
    #Use a groupby on the “movieId” and “rating” columns and take the count for each group
rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count()

    #Rename the “userId” column to “count.”
rating_counts = rating_counts.rename({'userId':'count'}, axis=1) 

    #Pivot this data so that movieId is the index, the columns will be all the rating values, and the rows will be the counts for each rating value.
rating_counts = rating_counts.pivot(index='movieId',columns='rating', values='count')

    #Rename the columns so they’re easier to understand.
rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
        
    #Merge the rating counts in movies_df:
    #Use a left merge, since we want to keep everything in movies_df.
movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

    #Fill in missing values
movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)  
print('Files have been transformed successfully.')
        
    #Load and update data into SQL tables
        
    #Establish connection string:
db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"

    #Create the database engine
engine = create_engine(db_string)
        
    #Delete data from movies table but keep tables
try:
        connection = psycopg2.connect(db_string)
        cursor = connection.cursor()
        sql_delete_query = "DELETE FROM movies"
        cursor.execute(sql_delete_query)
        connection.commit()
        count = cursor.rowcount
        print(count, "Record deleted successfully from movies")

except (Exception, psycopg2.Error) as error:
        print("Record from movies deleted. Complete.")
    
print("COMPLETE. Record deleted successfully from movies")    
    
       # Import the ratings data in chunks
      # Create a variable for the number of rows imported
rows_imported = 0
       # get the start_time from time.time()
start_time = time.time()
for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):

        # Print out the range of rows that are being imported
       print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
data.to_sql(name='ratings', con=engine, if_exists='append')

        # Increment the number of rows imported by the chunksize
rows_imported += len(data)

        # Print that the rows have finished importing and add elapsed time to print out
print(f'Done. {time.time() - start_time} total seconds elapsed')
print('The Load step is done.')

       # Call function
file_ETL('wikipedia-movies.json', 'movies_metadata.csv', 'ratings.csv')
   


# In[ ]:




