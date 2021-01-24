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
    total_df = pd.DataFrame()

    for fn in res_lst:
        if is_non_zero_file(fn):
            curr_df = pd.read_csv(fn)
            total_df = pd.concat([curr_df, total_df])

    total_df.drop_duplicates().to_csv("{final_dir}/consolidated_reviews.csv".format(final_dir=final_dir), index=False)


def combine_profiles(output_dir, final_dir):
    res_lst = glob.glob("{output_dir}/*.csv".format(output_dir=output_dir))
    total_df = pd.DataFrame()

    for fn in res_lst:
        if is_non_zero_file(fn):
            curr_df = pd.read_csv(fn)
            total_df = pd.concat([curr_df, total_df])

    total_df.drop_duplicates(subset='acc_num').to_csv("{final_dir}/consolidated_profiles.csv".format(final_dir=final_dir), index=False)


def combine_products(output_dir, final_dir):
    res_lst = glob.glob("{output_dir}/*.csv".format(output_dir=output_dir))
    total_df = pd.DataFrame()

    for fn in res_lst:
        if is_non_zero_file(fn):
            curr_df = pd.read_csv(fn)
            total_df = pd.concat([curr_df, total_df])

    total_df.drop_duplicates(subset='ASIN').to_csv("{final_dir}/consolidated_products.csv".format(final_dir=final_dir), index=False)


    # unique_asin_df = total_df.groupby('ASIN').size().reset_index(name='count').drop(columns=['count'])

    # columns = [column for column in total_df.columns if column != 'ASIN']
    # for column in columns:
    #     unique_asin_df[column] = np.nan

    # extract_non_nulls(unique_asin_df, total_df, columns)
    # unique_asin_df[unique_asin_df.ASIN != 'ASIN'].to_csv("{final_dir}/consolidated_products.csv".format(final_dir=final_dir), index=False)

