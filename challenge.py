#!/usr/bin/env python
# coding: utf-8




import json
import pandas as pd
import numpy as np
import os
import re
from sqlalchemy import create_engine
import psycopg2
from config import db_password
import time
import json


# In[2]:
#load wikipedia datas in JASON file list of dictionaries
file_dir = '.'

# Create a function that takes in three arguments and performs automated ETL pipeline.
def file_ETL(wiki, kaggle, ratings):
    with open(f'{file_dir}/{wiki}', mode='r') as file:
        wiki_movies_raw = json.load(file)
    kaggle_metadata = pd.read_csv(f'{file_dir}/{kaggle}', low_memory=False)
    ratings = pd.read_csv(f'{file_dir}/{ratings}', low_memory=False)
    print('The Extract step is done.')

    # Select the movies that have a Director and an IMDb link and do not have episodes.
    wiki_movies = [movie for movie in wiki_movies_raw if ('Director' in movie or 'Directed by' in movie)
                and 'imdb_link' in movie
                and 'No. of episodes' not in movie]
    # Create a DataFrame for movies
    wiki_movies_df = pd.DataFrame(wiki_movies)

    # Make a function to clean our data
    def clean_movie(movie):
        movie = dict(movie)  # create a non-destructive copy
        alt_titles = {}
        # combine alternate titles into one list
        for key in ['Also known as', 'Arabic', 'Cantonese', 'Chinese', 'French', 'Hangul', 'Hebrew', 'Hepburn',
                'Japanese', 'Literally', 'Mandarin', 'McCune–Reischauer', 'Original title', 'Polish',
                    'Revised Romanization', 'Romanized', 'Russian', 'Simplified', 'Traditional', 'Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # merge columns that have similar names and same data
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Directed by', 'Director')
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie

    # Make a list of cleaned movies
    clean_movies = [clean_movie(movie) for movie in wiki_movies]

    # Make a DataFrame of cleaned movies and check columns list
    wiki_movies_df = pd.DataFrame(clean_movies)

    # Extract the IMDb IDs from the links
    wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
    wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

    # Find the columns that have less than 90% null values
    wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
    wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]

    # Drop missing values for box_office
    box_office = wiki_movies_df['Box office'].dropna()

    # Join some lists
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    # To catch more values add possible space after dollar sign, make second 'i' in million optional for 'millon'
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
    # to look for ranges of numbers
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # Create a function to turn the extracted values into a numeric value
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

        

    # Extract the values and apply the parse_dollars function
    wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    # Remove the Box office column
    wiki_movies_df.drop('Box office', axis=1, inplace=True)

    # Create a budget variable
    budget = wiki_movies_df['Budget'].dropna()
    # Convert any lists to strings
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    # Remove any values between a dollar sign and a hyphen (for budgets given in ranges)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

    # Remove annotations
    budget = budget.str.replace(r'\[\d+\]\s*', '')

    # Parse the budget values
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
    # Drop the original Budget column
    wiki_movies_df.drop('Budget', axis=1, inplace=True)


    # Make a variable that holds the non-null values of Release date, converting lists to strings
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    # Create forms for 4 possible ways that dates appear
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'

    # Parse the dates
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
    # Drop the original Release date column
    wiki_movies_df.drop('Release date', axis=1, inplace=True)

    # Make a variable that holds the non-null values for Running Time, convert lists to strings
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

    # Extract the running time also matching the hour + minute patterns
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
    # Convert the strings to numeric values
    running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
    # Convert the hour capture groups and minute capture groups to minutes if the pure minutes group is zero
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
    # Drop the original Running time column
    wiki_movies_df.drop('Running time', axis=1, inplace=True)

    # Remove adult movies and any rows that got jumbled in Kaggle data
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')

    # Convert to Boolean column
    kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'

    # Convert object to numeric columns
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')

    # Convert release date to datetime type
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

    # Convert timestamp to datetime
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

    # Merge by IMDb ID and identify columns that are redundant
    movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])

    # Drop the row that has somehow merged The Holiday with From Here to Eternity
    movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01')
                                        & (movies_df['release_date_kaggle'] < '1965-01-01')].index)

    # Drop the title_wiki, release_date_wiki, Language, and Production company(s) columns
    movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

    # Make a function that fills in missing data for a column pair and then drops the redundant column
    def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
        df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
        df.drop(columns=wiki_column, inplace=True)

    # Run the function on the columns that need filling in
    fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
    fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
    fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')

    # Check to see if there are columns with only one value
    try:
        for col in movies_df.columns:
            lists_to_tuples = lambda x: tuple(x) if type(x) == list else x
            value_counts = movies_df[col].apply(lists_to_tuples).value_counts(dropna=False)
            num_values = len(value_counts)
            if num_values == 1:
                # Drop the column
                movies_df.drop(col, axis=1, inplace=True)
    except:
        print('No columns with only one value')

    # Reorder columns to make clearer for hackathon
    movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                        'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                        'genres','original_language','overview','spoken_languages','Country',
                        'production_companies','production_countries','Distributor','Producer(s)',
                        'Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on']]

    # Rename columns to be consistent
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
                    'Based on':'based_on'}, axis='columns',inplace=True)
    movies_df

    # Take a count for movieID and rating columns and rename userId to count
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count().rename({'userId':'count'}, axis=1)

    # Pivot the data so that movieId is the index, the columns will all be the rating values and the rows will be the counts
    rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count().rename({'userId':'count'}, axis=1).pivot(index='movieId',columns='rating', values='count')
    # Rename the columns to make easier to understand
    rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
    rating_counts

    # Use a left merge for movies_df and rating_counts
    movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')

    # Fill in missing values for movies that did not get a rating
    movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    print('The Transform step is done.')

    # Create a database engine
    db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
    engine = create_engine(db_string)

    # Import the DataFrame to a SQL table
    movies_df.to_sql(name='movies_challenge', con=engine, if_exists='append')
    print('The table movies_challenge is done.')

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