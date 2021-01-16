import glob
import os
from datetime import datetime
from subprocess import call

import configargparse
import pandas as pd
from stitch import combine_products, combine_profiles, combine_reviews


def get_reviews():
    """
    This function reads the scrape_reviews.csv and runs the amazon_reviews spider in batches of product url and pages.
    """
    path = args.file_path
    file_path = path + "/scrape_reviews.csv"
    
    # freq = args.freq
    output_dir = args.output_dir
    log_output = args.log_output

    url_df = pd.read_csv(file_path)
    for ind, row in url_df.iterrows():
        curr_url = row['url']
        curr_asin = curr_url.split('/')[4]
        print("Starting scraping for url: {}".format(curr_url))

        output_path = output_dir + "/reviews_{}.csv".format(curr_asin)
        #print(output_path)

        cmd = 'scrapy runspider spiders/amazon_reviews.py -o {} -a config="{},{},{}"'.format(output_path, curr_url, log_output, "main")
        call(cmd, shell=True)

        # total_pages = int(row['total_reviews']//10) + \
        #     1  # 1 page has 10 reviews
        # for start_page in range(1, total_pages, freq):
        #     end_page = min(start_page + freq + 1, total_pages + 1)
        #     output_path = output_dir + \
        #         "/reviews_{}_{}-{}.csv".format(curr_asin, start_page, end_page)
        #     scrape_cmd(output_path, curr_url, start_page,
        #                end_page, log_output, "main")


def get_outstanding_reviews():
    """
    Retries crawling the outstanding reviews that were unable to be retrieved in get_reviews. Updates the outstanding
    logs after every try.
    """
    log_output = args.log_output
    output_dir = args.output_dir
    num_retry = int(args.num_retry)

    cnt = 0
    while cnt < num_retry:
        cnt += 1
        print("Retrying outstanding: {iter}".format(iter=cnt))
        # Scrape remaining urls which failed previously
        output_path = output_dir + "/reviews_outstanding.csv"
        cmd = 'scrapy runspider spiders/amazon_reviews.py -o {} -a config="{},{},{}"'.format(output_path, "NA", log_output, "outstanding")
        call(cmd, shell=True)
        # scrape_cmd(output_path, "NA", "NA", "NA", log_output, "outstanding")

        # Update those urls which are scraped or not
        updated_df = pd.read_csv(log_output, names=["url", "num_items", "scraped"])
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
    # TODO: Include the paths in a config file/parse as arguments
    log_output = './output/logs/outstanding_profiles.csv'
    output_dir = './output/profiles'
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
    # TODO: Include the paths in a config file/parse as arguments
    log_output = './output/logs/outstanding_product_info.csv'
    output_dir = './output/raw_products_info'
    num_retry = int(args.num_retry)

    cnt = 0
    while cnt < num_retry:
        cnt += 1
        print("Retrying outstanding: {iter}".format(iter=cnt))
        # Scrape remaining profiles which failed previously
        output_path = output_dir + "/product_info_outstanding.csv"
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


# def scrape_cmd(output_path, curr_url, start_page, end_page, log_output, mode):
#     cmd = 'scrapy runspider spiders/amazon_reviews.py -o {} -a config="{},{},{},{},{}"' \
#         .format(output_path, curr_url, start_page, end_page, log_output, mode)
#     call(cmd, shell=True)


def get_profiles():
    """
    This function reads the scrape_profiles.csv and runs the amazon_product spider in batches
    """
    path = args.file_path
    profiles_path = path + "/scrape_profiles.csv"
    profiles_df = pd.read_csv(profiles_path)
    freq = 200
    max_rows = profiles_df.shape[0] + 1
    for start_row in range(0, max_rows, freq):
        end_row = min(start_row + freq, max_rows)

        # TODO: Use better naming convention or just stack it on same file
        cmd = 'scrapy runspider spiders/amazon_profiles.py -o output/profiles/profiles_{time}_{s_row}_{e_row}.csv '\
              '-a config="{profiles_path},output/logs/outstanding_profiles.csv,{s_row},{e_row},main"' \
            .format(profiles_path=profiles_path, s_row=start_row, e_row=end_row, time=datetime.now().strftime("%H%M%S"))
        call(cmd, shell=True)


def get_products():
    """
    This function reads the scrape_products.csv and runs the amazon_product spider in batches
    """
    path = args.file_path
    products_path = path + "/scrape_products.csv"
    products_df = pd.read_csv(products_path)
    freq = 200
    max_rows = products_df.shape[0] + 1
    for start_row in range(0, max_rows, freq):
        end_row = min(start_row + freq, max_rows)

        # TODO: Use better naming convention or just stack it on same file
        cmd = 'scrapy runspider spiders/amazon_products.py -o output/raw_products_info/products_info_{time}_{s_row}_{e_row}.csv '\
              '-a config="{products_path},output/logs/outstanding_product_info.csv,{s_row},{e_row},main"' \
            .format(products_path=products_path, s_row=start_row, e_row=end_row, time=datetime.now().strftime("%H%M%S"))
        call(cmd, shell=True)


def create_urls():
    """
    This function creates the amazon urls for scraping reviews and products from a csv containing
    amazon product ASINs.
    """
    asin_file_path = "./data/product_asin.csv"
    asin_df = pd.read_csv(asin_file_path, header=0)
    asins = asin_df.asin.tolist()
    product_urls = [f"https://www.amazon.com/dp/{asin}" for asin in asins]
    review_urls = [
        f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews" for asin in asins]
    product_df = pd.DataFrame(product_urls, columns=["url"])
    product_df.to_csv('./data/scrape_products.csv', index=False)
    review_df = pd.DataFrame(review_urls, columns=["url"])
    review_df.to_csv('./data/scrape_reviews.csv', index=False)


def get_profile_urls():
    """
    This function creates the amazon urls for scraping profiles from the scraped reviews that are stored
    in the output/raw folder.
    """
    reviews_file_path = args.output_dir
    path = args.file_path
    profiles_path = path + "/scrape_profiles.csv"

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


def parse_args():
    # TODO: Save logs/cmd line outputs in a file
    # TODO: Clean up the command functions (merge)
    """
    This function creates a parser object that takes in arguments from config.yml file. These arguments can be
    overwritten using the add arguments function when calling the module from the command line.
    """
    p = configargparse.ArgumentParser(default_config_files=[
                                      './config.yml'], config_file_parser_class=configargparse.YAMLConfigFileParser)

    p.add_argument("-m", "--scraping_mode",
                   help="Specify mode of scraper ['reviews','profiles', 'products']")
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

    return p.parse_args()


if __name__ == "__main__":
    # read arguments for scraper
    args = parse_args()

    # create urls to scrape reviews and products from a csv containing product ASINs
    create_urls()

    # Scrape reviews
    # TODO: Update to include rotation for googlebots2.1 in the useragents (See documentation)
    get_reviews()
    # get_outstanding_reviews()
    # combine_reviews(args.output_dir, args.final_output)

    # Scrape products
    # TODO: Update to include rotation for googlebots2.1 in the useragents (See documentation)
    # get_products()
    #     get_outstanding_products()
    #     combine_products(args.output_dir, args.final_output)

    # # obtain profile urls from scraped reviews in raw
    # get_profile_urls()

    # Scrape profiles
    # get_profiles()
    # get_outstanding_profiles()
    # combine_profiles('./output/profiles', './output/customers')

    # # TODO: Add arguments/config to allow changing of settings.py settings
    # # TODO: Update readme to include usage examples, parameter explanation and installation instructions
