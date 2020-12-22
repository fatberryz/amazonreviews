import configargparse
from subprocess import call
import pandas as pd
from datetime import datetime

from stitch import combine_profiles
from stitch import combine_reviews
from stitch import combine_products


def get_reviews():
    """
    This function reads the scrape_urls.csv and runs the amazon_reviews spider in batches of product url and pages.
    """
    file_path = args.file_path
    freq = args.freq
    output_dir = args.output_dir
    log_output = args.log_output

    url_df = pd.read_csv(file_path)
    for ind, row in url_df.iterrows():
        curr_url = row['url']
        print("Starting scraping for url: {}".format(curr_url))
        curr_asin = curr_url.split('/')[5]
        total_pages = int(row['total_reviews']//10) + 1  # 1 page has 10 reviews
        for start_page in range(1, total_pages, freq):
            end_page = min(start_page + freq + 1, total_pages + 1)
            output_path = output_dir + "/reviews_{}_{}-{}.csv".format(curr_asin, start_page, end_page)
            scrape_cmd(output_path, curr_url, start_page, end_page, log_output, "main")


def get_outstanding_reviews():
    """
    Retries crawling the outstanding reviews that were unable to be retrieved in get_reviews. Updates the outstanding
    logs after every try.
    """
    log_output = args.log_output
    output_dir = args.output_dir
    num_retry = args.num_retry

    cnt = 0
    while cnt < num_retry:
        cnt += 1
        print("Retrying outstanding: {iter}".format(iter=cnt))
        # Scrape remaining urls which failed previously
        output_path = output_dir + "/reviews_outstanding.csv"
        scrape_cmd(output_path, "NA", "NA", "NA", log_output, "outstanding")

        # Update those urls which are scraped or not
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

def scrape_cmd(output_path, curr_url, start_page, end_page, log_output, mode):
    cmd = 'scrapy runspider spiders/amazon_reviews.py -o {} -a config="{},{},{},{},{}"' \
        .format(output_path, curr_url, start_page, end_page, log_output, mode)
    call(cmd, shell=True)


def get_profiles():
    """
    This function reads the scrape_profiles.csv and runs the amazon_product spider in batches
    """
    profiles_path = './data/scrape_profiles.csv'
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
    products_path = './data/scrape_products.csv'
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

def parse_args():
    # Make parser object
    # p = argparse.ArgumentParser(description=__doc__,
    #                             formatter_class=argparse.RawDescriptionHelpFormatter)

    # TODO: read arguments from configuration file instead
    # TODO: Save logs/cmd line outputs in a file
    # TODO: Clean up the command functions (merge)
    p = configargparse.ArgumentParser(default_config_files=['./config.yml'],config_file_parser_class=configargparse.YAMLConfigFileParser)

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

    # opt = p.parse_args()
    # print(opt)

    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    # TODO: Add function to combine csv for profiles
    # TODO: Add arguments/config to allow changing of settings.py settings
    # TODO: Update readme to include usage examples, parameter explanation and installation instructions
    if args.scraping_mode == "profiles":
        # TODO: Need to read from the reviews folder, then de-dup and feed to this module
        get_profiles()
        get_outstanding_profiles()
        combine_profiles('./output/profiles', './output/customers')
    if args.scraping_mode == "reviews":
        # TODO: Update to include roptation for googlebots2.1 in the useragents (See documentation)
        get_reviews()
        get_outstanding_reviews()
        combine_reviews(args.output_dir, args.final_output)
    if args.scraping_mode == "products":
        # TODO: Update to include roptation for googlebots2.1 in the useragents (See documentation)
        get_products()
        get_outstanding_products()
        combine_products(args.output_dir, args.final_output)
