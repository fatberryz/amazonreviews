import os
import glob
import pandas as pd
from pathlib import Path


def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


def combine_reviews(output_dir, final_dir):
    extension = 'csv'
    filename_lst = [i for i in glob.glob('{}/*.{}'.format(output_dir, extension))]

    # Finding unique product prefixes
    filtered_lst = ['_'.join(Path(i).stem.split('_')[:2]) for i in filename_lst]
    filtered_lst = list(dict.fromkeys(filtered_lst))

    if "reviews_outstanding" in filtered_lst and is_non_zero_file(output_dir + "/reviews_outstanding.csv"):
        filtered_lst.remove("reviews_outstanding")
        outstanding_df = pd.read_csv(output_dir + "/reviews_outstanding.csv")
        outstanding_df = outstanding_df.drop_duplicates(keep="first")
        for asin in outstanding_df['ASIN'].drop_duplicates(keep="first"):
            curr_name = "{}/reviews_{}_outstanding.csv".format(output_dir, asin)
            curr_df = outstanding_df[outstanding_df['ASIN'] == asin]
            curr_df.to_csv(curr_name, index=False, encoding='utf-8-sig')
        os.remove(output_dir + "/reviews_outstanding.csv")

    filtered_lst = ['_'.join(Path(i).stem.split('_')[:2]) for i in filename_lst]
    filtered_lst = list(dict.fromkeys(filtered_lst))
    filtered_lst.remove("reviews_ASIN")
    for filename in filtered_lst:
        try:
            search_lst = [i for i in glob.glob('{}/{}*.{}'.format(output_dir, filename, extension))]
            combined_csv = pd.concat([pd.read_csv(f) for f in search_lst if os.stat(f).st_size > 0 ])
            combined_csv = combined_csv.drop_duplicates()
            combined_csv.to_csv("{}/{}.csv".format(final_dir, filename), index=False, encoding='utf-8-sig')
            print("Total rows scraped for {}: {}".format(filename, len(combined_csv)))
        except Exception as e:
            print('Failed to combine: '+ str(e))


def combine_profiles(output_dir, final_dir):
    res_lst = glob.glob("{output_dir}/*.csv".format(output_dir=output_dir))
    total_df = pd.DataFrame()

    for fn in res_lst:
        if is_non_zero_file(fn):
            curr_df = pd.read_csv(fn)
            total_df = pd.concat([curr_df, total_df])

    total_df.drop_duplicates(subset='acc_num').to_csv("{final_dir}/consolidated_profiles.csv".format(final_dir=final_dir), index=False)