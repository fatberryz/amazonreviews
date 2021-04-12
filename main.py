import csv
import glob
import json
import os
from datetime import datetime
from subprocess import call

import configargparse
import pandas as pd

from gcpFunctions import create_bq_client, upload_csv_as_df
from stitch import combine_products, combine_profiles, combine_reviews


def get_reviews():
    """
    This function reads the scrape_reviews.csv and runs the amazon_reviews spider in batches of product url and pages.
    """
    input_data_path = args.file_path
    file_path = input_data_path + "/scrape_reviews.csv"
    output_dir = args.output_dir + "/reviews"
    log_output = args.log_output + 'outstanding_reviews.csv'
    tracker_dir = args.tracker_output
    tracker_path = tracker_dir + 'reviews_tracker.json'

    url_df = pd.read_csv(file_path)
    for ind, row in url_df.iterrows():
        curr_url = row['url']
        curr_asin = curr_url.split('/')[4]
        
        output_path = output_dir + "/reviews_{}.csv".format(curr_asin)

        cmd = 'scrapy runspider spiders/amazon_reviews.py -o {} -a config="{},{},{},{}"'.format(output_path, curr_url, log_output, "main", tracker_path)
        call(cmd, shell=True)
        

def get_outstanding_reviews():
    """
    Retries crawling the outstanding reviews that were unable to be retrieved in get_reviews. Updates the outstanding
    logs after every try.
    """
    log_output = args.log_output + "outstanding_reviews.csv"
    output_dir = args.output_dir + "/reviews"
    num_retry = int(args.num_retry)
    tracker_dir = args.tracker_output
    tracker_path = tracker_dir + 'reviews_tracker.json'

    cnt = 0
    while cnt < num_retry:
        cnt += 1
        print("Retrying outstanding: {iter}".format(iter=cnt))
        # Scrape remaining urls which failed previously
        output_path = output_dir + "/reviews_outstanding.csv"

        cmd = 'scrapy runspider spiders/amazon_reviews.py -o {} -a config="{},{},{},{}"'.format(output_path, "NA", log_output, "outstanding", tracker_path)
        call(cmd, shell=True)

        # Update those urls which are scraped or not (0 or 1 in scraped column)
        updated_df = pd.read_csv(log_output)
        outstanding_df = updated_df.groupby('url').filter(lambda x: len(x) > 1)
        outstanding_df = outstanding_df.drop_duplicates('url', keep='last')
        cleared_df = updated_df.groupby('url').filter(lambda x: len(x) == 1)
        cleared_df['scraped'] = 1
        updated_df = pd.concat([cleared_df, outstanding_df])

        updated_df.to_csv(log_output, index=False)


def get_outstanding_profiles():
    """
    Retries crawling the outstanding profiles that were unable to be retrieved in get_profiles. Updates the outstanding
    logs after every try.
    """
    log_output = args.log_output + "outstanding_profiles.csv"
    output_dir = args.output_dir + "/profiles"
    num_retry = int(args.num_retry)

    cnt = 0
    while cnt < num_retry:
        cnt += 1
        print("Retrying outstanding: {iter}".format(iter=cnt))
        # Scrape remaining profiles which failed previously
        output_path = output_dir + "/profiles_outstanding.csv"

        cmd = 'scrapy runspider spiders/amazon_profiles.py -o {out_path} -a config="_,{log_output},_,_,outstanding"' \
            .format(out_path=output_path, log_output=log_output)
        call(cmd, shell=True)

        # Update those urls which are scraped or not
        updated_df = pd.read_csv(log_output)
        outstanding_df = updated_df.groupby('url').filter(lambda x: len(x) > 1)
        outstanding_df = outstanding_df.drop_duplicates('url', keep='last')
        cleared_df = updated_df.groupby('url').filter(lambda x: len(x) == 1)
        cleared_df['scraped'] = 1
        updated_df = pd.concat([cleared_df, outstanding_df])

        updated_df.to_csv(log_output, index=False)


def get_outstanding_products():
    """
    Retries crawling the outstanding products that were unable to be retrieved in get_products. Updates the outstanding
    logs after every try.
    """
    log_output = args.log_output + "outstanding_products.csv"
    output_dir = args.output_dir + "/products"
    num_retry = int(args.num_retry)

    cnt = 0
    while cnt < num_retry:
        cnt += 1
        print("Retrying outstanding: {iter}".format(iter=cnt))
        # Scrape remaining profiles which failed previously
        output_path = output_dir + "/products_outstanding.csv"

        cmd = 'scrapy runspider spiders/amazon_products.py -o {out_path} -a config="_,{log_output},_,_,outstanding"' \
            .format(out_path=output_path, log_output=log_output)
        call(cmd, shell=True)

        # Update those urls which are scraped or not
        updated_df = pd.read_csv(log_output)
        outstanding_df = updated_df.groupby('url').filter(lambda x: len(x) > 1)
        outstanding_df = outstanding_df.drop_duplicates('url', keep='last')
        cleared_df = updated_df.groupby('url').filter(lambda x: len(x) == 1)
        cleared_df['scraped'] = 1
        updated_df = pd.concat([cleared_df, outstanding_df])

        updated_df.to_csv(log_output, index=False)


def get_profiles():
    """
    This function reads the scrape_profiles.csv and runs the amazon_product spider in batches
    """
    input_data_path = args.file_path
    profiles_path = input_data_path + "/scrape_profiles.csv"
    log_output = args.log_output + "outstanding_profiles.csv"
    output_dir = args.output_dir + "/profiles"
    profiles_df = pd.read_csv(profiles_path)
    freq = 200
    max_rows = profiles_df.shape[0] + 1

    for start_row in range(0, max_rows, freq):
        end_row = min(start_row + freq, max_rows)

        output_path = output_dir + '/profiles_{time}_{s_row}_{e_row}.csv'\
            .format(s_row=start_row, e_row=end_row, time=datetime.now().strftime("%H%M%S"))

        cmd = 'scrapy runspider spiders/amazon_profiles.py -o {output_path} '\
              '-a config="{profiles_path},{log_file},{s_row},{e_row},main"' \
            .format(output_path=output_path, profiles_path=profiles_path, log_file=log_output, s_row=start_row, e_row=end_row, time=datetime.now().strftime("%H%M%S"))
        call(cmd, shell=True)


def get_products():
    """
    This function reads the scrape_products.csv and runs the amazon_product spider in batches
    """
    input_data_path = args.file_path
    products_path = input_data_path + "/scrape_products.csv"
    log_output = args.log_output + "outstanding_products.csv"
    output_dir = args.output_dir + "/products"
    products_df = pd.read_csv(products_path)

    freq = 200
    max_rows = products_df.shape[0] + 1
    for start_row in range(0, max_rows, freq):
        end_row = min(start_row + freq, max_rows)
        output_path = output_dir + '/products_info_{time}_{s_row}_{e_row}.csv'\
            .format(s_row=start_row, e_row=end_row, time=datetime.now().strftime("%H%M%S"))

        cmd = 'scrapy runspider spiders/amazon_products.py -o {output_path} '\
              '-a config="{products_path}, {log_file},{s_row},{e_row},main"'\
            .format(output_path=output_path, log_file=log_output, products_path=products_path, s_row=start_row, e_row=end_row)
        call(cmd, shell=True)


def create_urls():
    """
    This function creates the amazon urls for scraping reviews and products from a csv containing
    amazon product ASINs.
    """
    data_basepath = args.file_path 
    asin_file_path = data_basepath  + "/product_asin.csv"
    asin_df = pd.read_csv(asin_file_path, header=0)
    asins = asin_df.asin.tolist()
    product_urls = [f"https://www.amazon.com/dp/{asin}" for asin in asins]
    review_urls = [
        f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews&sortBy=recent" for asin in asins]
    product_df = pd.DataFrame(product_urls, columns=["url"])
    product_df.to_csv( data_basepath + '/scrape_products.csv', index=False)
    review_df = pd.DataFrame(review_urls, columns=["url"])
    review_df.to_csv(data_basepath + '/scrape_reviews.csv', index=False)


def get_profile_urls():
    """
    This function creates the amazon urls for scraping profiles from the scraped reviews that are stored
    in the output/raw folder. 
    """
    reviews_file_path = args.final_output + "/reviews"
    input_data_path = args.file_path
    profiles_path = input_data_path + "/scrape_profiles.csv"

    # read all reviews in file path into one df
    all_files = glob.glob(os.path.join(reviews_file_path, "*.csv"))    # advisable to use os.path.join as this makes concatenation OS independent
    df_from_each_file = (pd.read_csv(f) for f in all_files)
    concatenated_df = pd.concat(df_from_each_file, ignore_index=True)

    # remove rows with empty profile links if it exists (profile that left the review might have been removed or deleted)
    concatenated_df = concatenated_df.dropna(subset=['profile_link'])
    # extract profile_links column from concatenated df
    profile_links = concatenated_df.profile_link.tolist()
    # remove duplicate profile links
    distinct_profile_links = (set(profile_links))

    # generate profile urls to scrape 
    profile_urls = [f"https://www.amazon.com{profile_link}" for profile_link in distinct_profile_links]
    profile_df = pd.DataFrame(profile_urls, columns=["url"])
    profile_df.to_csv(profiles_path, index=False)  


def upload_consolidated_csvs(svc_account_credential_file_path, project_name, target_dataset):
    """
    This function uploads the consolidated scraped items into GBQ.

    Change credential file path / table names accordingly as needed
    """
    # project parameters
    consolidated_dir = args.final_output

    ## upload reviews
    reviews_file_path = consolidated_dir + ("/reviews/consolidated_reviews.csv")
    # add try-except logic in case consolidated file does not exist
    try:
        # upload if file > 1kb
        if os.stat(reviews_file_path).st_size > 1000:
            upload_csv_as_df(svc_account_credential_file_path, project_name, target_dataset, "reviews", reviews_file_path)
        else:
            print("There is no reviews in the consolidated file to be uploaded")
    except FileNotFoundError:
        print(f"{reviews_file_path} does not exist")


    ## upload products
    products_file_path = consolidated_dir + ("/products/consolidated_products.csv")
    # add try-except logic in case consolidated file does not exist
    try:
        if os.stat(products_file_path).st_size > 1000:
            upload_csv_as_df(svc_account_credential_file_path, project_name, target_dataset, "products", products_file_path)
        else:
            print("There is no products in the consolidated file to be uploaded")
    except FileNotFoundError:
        print(f"{products_file_path} does not exist")

    ## upload profiles
    profiles_file_path = consolidated_dir + ("/profiles/consolidated_profiles.csv")
    # add try-except logic in case consolidated file does not exist
    try:
        if os.stat(profiles_file_path).st_size > 1000:
            upload_csv_as_df(svc_account_credential_file_path, project_name, target_dataset, "profiles", profiles_file_path)
        else:
            print("There is no profiles in the consolidated file to be uploaded")
    except FileNotFoundError:
        print(f"{profiles_file_path} does not exist")

def clear_output_folders():
    """
    This function helps to clear all the output folders of the csv files and then recreates the outstanding item files
    in the log folder with the headers - 'url', 'num_items' and 'scraped'
    """
    # remove csv files in output folder and its sub-folders recursively
    files = glob.glob('./output/**/*.csv', recursive=True)
    for f in files:
        try:
            os.remove(f)
        except OSError as e:
            print("Error: %s : %s" % (f, e.strerror))
    print("csv files in output folder have been removed")

    # recreate csv files in log folder
    file_path = args.log_output
    file_names = ['outstanding_reviews.csv','outstanding_profiles.csv','outstanding_products.csv']
    for fn in file_names:
        csv_name = file_path + fn
        with open(csv_name, 'w') as csvfile:
            fieldnames = ['url', 'num_items', 'scraped']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
    print(f"log files with headers have been created in {file_path} folder")

# Update counter of web scrape tool each time it is activated (queue 1 with get_review_profile)
def update_counter_file():
    # get current date
    current_date = datetime.today().strftime('%Y-%m-%d')

    # load json counter file to read/write to
    with open('../webscrap-website/dashboard_data/webscrape_counter.json', 'r+') as infile:
        # load current file unless it is empty which causes the jsondecode error
        try:
            current_counts = json.load(infile)
            print("JSON WEBSCRAPE COUNTER FILE LOADED SUCCESSFULLY", current_counts)
            if current_date in current_counts:
                current_counts[f'{current_date}'] += 1
            else:
                current_counts[f'{current_date}'] = 1
        except ValueError: #catches json decode error
            # initialize the first time when the json counter file is empty
            print("JSON WEBSCRAPE COUNTER FILE NOT LOADED DUE TO VALUE ERROR")
            current_counts = {}
            current_counts[f'{current_date}'] = 1
        with open('../webscrap-website/dashboard_data/webscrape_counter.json', 'w') as outfile:
            # write updated count to counter file
            json.dump(current_counts, outfile)
            infile.close()
            outfile.close()
        

def parse_args():
    # TODO: Save logs/cmd line outputs in a file
    # TODO: Clean up the command functions (merge)
    """
    This function creates a parser object that takes in arguments from config.yml file. These arguments can be 
    overwritten using the add arguments function when calling the module from the command line.
    """
    p = configargparse.ArgumentParser(default_config_files=[
                                      './config.yml'], config_file_parser_class=configargparse.YAMLConfigFileParser)
    p.add_argument("-f", "--file_path",
                   help="Csv file that contains the review urls and no. reviews")
    p.add_argument("-n", "--freq", type=int,
                   help="Number of pages to crawl before re-running the script to refresh proxies.")
    p.add_argument("-o", "--output_dir",
                   help="Output directory of scraped reviews")
    p.add_argument("-lo", "--log_output",
                   help="Output directory of log (Outstanding urls)")
    p.add_argument("-fo", "--final_output",
                   help="Output directory of final scraped products (After stitching)")
    p.add_argument("-nr", "--num_retry",
                   help="Number of retries of scraping outstanding items")
    p.add_argument("-td", "--tracker_output",
                help="Output directory for tracker json file for scraped items")

    return p.parse_args()


if __name__ == "__main__":
    # read arguments for scraper
    args = parse_args()

    # # create urls to scrape reviews and products from a csv containing product ASINs
    # create_urls()

    #Scrape reviews 
    #TODO: Update to include rotation for googlebots2.1 in the useragents (See documentation)
    get_reviews()
    get_outstanding_reviews()
    combine_reviews((args.output_dir + '/reviews'), (args.final_output + '/reviews'))

    # #  Scrape products
    # #  TODO: Update to include rotation for googlebots2.1 in the useragents (See documentation)
    # get_products()
    # get_outstanding_products()
    # combine_products((args.output_dir + '/products'), (args.final_output + '/products'))

    # # Obtain profile urls from scraped reviews in raw
    # get_profile_urls()

    # # Scrape profiles
    # get_profiles()
    # get_outstanding_profiles()
    # combine_profiles((args.output_dir + '/profiles'), (args.final_output + '/profiles'))
    
    # #Upload consolidated CSVs into GBQ
    # upload_consolidated_csvs('./credential_file.json', 'crafty-chiller-276910', 'scraped_items_test' )

    # # Clear output
    # clear_output_folders()

    # update webscrape counter file im webscrap-website repo 
    update_counter_file()

    # # TODO: Add arguments/config to allow changing of settings.py settings --> take in params from website
    # # TODO: Update readme to include usage examples, parameter explanation and installation instructions
