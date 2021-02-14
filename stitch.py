import os
import glob
import pandas as pd
import numpy as np


def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


def extract_non_nulls(final_df, df, columns):
    for column in columns:
        for index, row in df.iterrows():
            if row[column] != np.nan:
                final_df.loc[final_df['ASIN'] == row['ASIN'], column] = row[column]


def combine_reviews(output_dir, final_dir):
    res_lst = glob.glob("{output_dir}/*.csv".format(output_dir=output_dir))
    col_names = [
        'ASIN',
        'comment',
        'date',
        'date_scraped',
        'profile_image',
        'profile_link',
        'profile_name',
        'review_images',
        'stars',
        'style',
        'title',
        'verified',
        'voting'
    ]
    total_df = pd.DataFrame(columns=col_names)

    for fn in res_lst:
        if is_non_zero_file(fn):
            curr_df = pd.read_csv(fn)
            # remove duplicates from multiple retries of outstanding reviews
            curr_df.drop_duplicates(keep='first')
            curr_df.columns = total_df.columns
            total_df = pd.concat([curr_df, total_df], ignore_index=True)
        # remove empty rows and extra rows containing column headers
        total_df = total_df.dropna(subset=['ASIN'])
        total_df = total_df[total_df.ASIN != 'ASIN']
        total_df.drop_duplicates(keep='first').to_csv("{final_dir}/consolidated_reviews.csv".format(final_dir=final_dir), index=False, header=col_names)


def combine_profiles(output_dir, final_dir):
    res_lst = glob.glob("{output_dir}/*.csv".format(output_dir=output_dir))
    col_names = [
        'json_data',
        'acc_num',
        'name',
        'occupation',
        'location',
        'description',
        'badges',
        'ranking',
        'date_scraped'
    ]
    total_df = pd.DataFrame(columns=col_names)
    for fn in res_lst:
        if is_non_zero_file(fn):
            curr_df = pd.read_csv(fn)
            # remove duplicates from multiple retries of outstanding profiles
            curr_df.drop_duplicates(keep='first')
            curr_df.columns = total_df.columns
            total_df = pd.concat([curr_df, total_df], ignore_index=True)
        # remove empty rows and extra rows containing column headers
        total_df = total_df.dropna(subset=['acc_num'])
        total_df = total_df[total_df.json_data != 'json_data']
        total_df.drop_duplicates(keep='first').to_csv("{final_dir}/consolidated_profiles.csv".format(final_dir=final_dir), index=False, header=col_names)


def combine_products(output_dir, final_dir):
    res_lst = glob.glob("{output_dir}/*.csv".format(output_dir=output_dir))
    col_names = [
    'ASIN',
    'availability',
    'date_scraped',
    'description',
    'price',
    'rating'
    ]
    total_df = pd.DataFrame(columns=col_names)
    for fn in res_lst:
        if is_non_zero_file(fn):
            curr_df = pd.read_csv(fn)
            # remove duplicates from multiple retries of outstanding products
            curr_df.drop_duplicates(keep='first')
            curr_df.columns = total_df.columns
            total_df = pd.concat([curr_df, total_df], ignore_index=True)
        # remove empty rows and extra rows containing column headers
        total_df = total_df.dropna(subset=['ASIN'])
        total_df = total_df[total_df.ASIN != 'ASIN']
        total_df.drop_duplicates(keep='first').to_csv("{final_dir}/consolidated_products.csv".format(final_dir=final_dir), index=False, header=col_names)