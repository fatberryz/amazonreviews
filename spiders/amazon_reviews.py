# -*- coding: utf-8 -*-
# Importing Scrapy Library
import json
import platform
import random
import time
from datetime import datetime

import pandas as pd

import scrapy
from datetime import datetime
from scrapy import signals
from scrapy.exceptions import CloseSpider

# To allow Mac to load spider module from parent folder
if platform.system() == "Darwin" or platform.system() == "Linux":
    import os, sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from items import AmazonReviewsItem
else:
    from amazonreviews.items import AmazonReviewsItem

# Creating a new class to implement Spider
class AmazonReviewsSpider(scrapy.Spider):
    # Spider name
    name = 'amazon_reviews'
    custom_settings = {}

    def __init__(self, *args, **kwargs):
        super(AmazonReviewsSpider, self).__init__(*args, **kwargs)

        config = kwargs['config'].split(',')
        self.log_output = config[1]
        mode = config[2]
        start_urls = []
        self.tracker_path = config[3]

        if mode == "outstanding":
            outstanding_df = pd.read_csv(self.log_output)
            outstanding_df = outstanding_df[outstanding_df['scraped'] == 0]
            for raw_url in outstanding_df['url']:
                format_url = raw_url.replace("start_requests/item_scraped_count/", "")
                start_urls.append(format_url)
            self.start_urls = start_urls

        if mode == "main":
            url_name = config[0]
            start_urls.append(url_name)
            self.start_urls = start_urls
        self.logger.info(self.start_urls)

    # Domain names to scrape
    allowed_domains = ['amazon.co.uk', 'amazon.com']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AmazonReviewsSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        """
        Executes on spider closed. Checks which items have not been scraped and updates the log file.
        :param spider: takes in spider instance
        """
        stats = spider.crawler.stats.get_stats()
        prefix = 'start_requests/item_scraped_count/'
        with open(self.log_output, "a") as log_file:
            for url in self.start_urls:
                if prefix + url in stats and stats[prefix + url] != 10:
                    # Sample scrapy stat for full 500 review scrape:
                    # 'start_requests/item_scraped_count/https://www.amazon.com/product-reviews/B004BCXAM8/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews': 5000,
                    log_file.write(prefix+url + "," + str(stats[prefix + url]) + ',0' + '\n') # url + num_items scraped + scraped [0 or 1]
                if prefix + url not in stats:
                    log_file.write(prefix + url + ",0" + ",0" + '\n')

    # Defining a Scrapy parser
    def parse(self, response):
        items = AmazonReviewsItem()
        date_scraped = datetime.today().strftime('%Y-%m-%d')

        data = response.css('#cm_cr-review_list')
        # Collecting user reviews
        print("Current URL being scraped: \n\t", response.request.url)
        reviews = data.css('div[data-hook="review"]')
        ASIN = response.request.url.split('/')[4]
        # for URLs after the first page with the page numbers
        if '?' in ASIN:
            ASIN = ASIN.split('?')[0]
        print(ASIN)

        # Boolean value to track if it should go to the next page or not
        reached_old_reviews = False

        # identify which product reviews have been scraped before 
        with open(self.tracker_path, 'r+') as reviews_file:
            reviews_data = json.load(reviews_file)
            #print(reviews_data)

            # if review has been scraped before, scrape new reviews, else scrape all reviews
            if ASIN not in reviews_data:
                # Product has not been scraped before - scrape all reviews
                print(f"Product ASIN - {ASIN} has not been scraped before. Scraping all reviews")

                # Combining the results
                for review in reviews:
                    if ''.join(review.xpath('.//i[@data-hook="review-star-rating"]//text()').extract()).strip() != '':
                        stars = ''.join(review.xpath('.//i[@data-hook="review-star-rating"]//text()').extract()).strip()
                    else:
                        stars = ''.join(review.xpath('.//i[@data-hook="cmps-review-star-rating"]//text()').extract()).strip()

                    profile_name = ''.join(review.xpath('.//span[@class="a-profile-name"]//text()').extract()).strip()
                    profile_link = ''.join(review.xpath('.//div[@data-hook="genome-widget"]//a/@href').extract()).strip()
                    profile_image = ''.join(review.xpath('.//div[@class="a-profile-avatar"]//img/@data-src').extract()).strip()
                    title = ''.join(review.xpath('.//a[@data-hook="review-title"]//text()').extract()).strip()
                    date =  ''.join(review.xpath('.//span[@data-hook="review-date"]//text()').extract()).strip()
                    style =  ''.join(review.xpath('.//a[@data-hook="format-strip"]//text()').extract()).strip()
                    verified = ''.join(review.xpath('.//span[@data-hook="avp-badge"]//text()').extract()).strip()
                    comment =  ''.join(review.xpath('.//span[@data-hook="review-body"]//text()').extract()).strip()
                    voting = ''.join(review.xpath('.//span[@data-hook="review-voting-widget"]//text()').extract()).strip()
                    review_images = len(review.xpath('.//div[@class="review-image-tile-section"]//img'))
                    # for URLs after the first page with the page numbers
                    if '?' in ASIN:
                        ASIN = ASIN.split('?')[0]

                    items["stars"] = stars
                    items["profile_name"] = profile_name
                    items["profile_link"] = profile_link
                    items["profile_image"] = profile_image
                    items["title"] = title
                    items["date"] = date
                    items["style"] = style
                    items["verified"] = verified
                    items["comment"] = comment
                    items["voting"] = voting
                    items["review_images"] = review_images
                    items["ASIN"] = ASIN
                    items["date_scraped"] = date_scraped

                    yield items

                # next page url
                next_page_partial_url = response.xpath('//li[@class="a-last"]/a/@href').extract_first()

                if next_page_partial_url and not reached_old_reviews:
                    # remove name of product in front
                    partial_url = str(next_page_partial_url).split("/product-reviews", 1)[1]
                    # concat partial_url with base url for product reviews
                    next_page_url = "https://www.amazon.com/product-reviews" + str(partial_url)

                    # continue following the next page link as long as there is content to scrape in the next page
                    yield scrapy.Request(next_page_url, callback=self.parse)
                    # introduce random delay between requests to reduce risk of being blocked
                    time.sleep(random.randint(4, 8))
                
                elif next_page_partial_url and reached_old_reviews:
                    print("Reached the point of old reviews. Stop scraping for current product review")
                    reviews_data[ASIN] = date_scraped
                    with open(self.tracker_path, 'w') as reviews_file:
                        json.dump(reviews_data, reviews_file)
                    raise CloseSpider('termination condition met') 

                else:
                    print("No more next review page button. Stop scraping for current product review")
                    reviews_data[ASIN] = date_scraped
                    with open(self.tracker_path, 'w') as reviews_file:
                        json.dump(reviews_data, reviews_file)
                    raise CloseSpider('termination condition met') 
            else:
                # product review has already been scraped before - scrape new reviews only
                date_of_last_scraped = reviews_data[ASIN]
                print(f"Product with ASIN {ASIN} has been scraped before on {date_of_last_scraped}")

                # Combining the results
                for review in reviews:
                    date = ''.join(review.xpath('.//span[@data-hook="review-date"]//text()').extract()).strip()
                    review_date_string = date.split('on')[1]
                    review_date = datetime.strptime(review_date_string, ' %B %d, %Y').strftime('%Y-%m-%d')
                    # reached the page of old scraped reviews, stop scraping next pages
                    if review_date < date_of_last_scraped:
                        reached_old_reviews = True
                    else:
                        # have not reached old scraped page reviews, continue scraping next page
                        if ''.join(review.xpath('.//i[@data-hook="review-star-rating"]//text()').extract()).strip() != '':
                            stars = ''.join(review.xpath('.//i[@data-hook="review-star-rating"]//text()').extract()).strip()
                        else:
                            stars = ''.join(review.xpath('.//i[@data-hook="cmps-review-star-rating"]//text()').extract()).strip()
                        profile_name =  ''.join(review.xpath('.//span[@class="a-profile-name"]//text()').extract()).strip()
                        profile_link = ''.join(review.xpath('.//div[@data-hook="genome-widget"]//a/@href').extract()).strip()
                        profile_image = ''.join(review.xpath('.//div[@class="a-profile-avatar"]//img/@data-src').extract()).strip()
                        title = ''.join(review.xpath('.//a[@data-hook="review-title"]//text()').extract()).strip()
                        style = ''.join(review.xpath('.//a[@data-hook="format-strip"]//text()').extract()).strip()
                        verified = ''.join(review.xpath('.//span[@data-hook="avp-badge"]//text()').extract()).strip()
                        comment = ''.join(review.xpath('.//span[@data-hook="review-body"]//text()').extract()).strip()
                        voting = ''.join(review.xpath('.//span[@data-hook="review-voting-widget"]//text()').extract()).strip()
                        review_images = len(review.xpath('.//div[@class="review-image-tile-section"]//img'))
                        # for URLs after the first page with the page numbers
                        if '?' in ASIN:
                            ASIN = ASIN.split('?')[0]

                        items["stars"] = stars
                        items["profile_name"] = profile_name
                        items["profile_link"] = profile_link
                        items["profile_image"] = profile_image
                        items["title"] = title
                        items["date"] = date
                        items["style"] = style
                        items["verified"] = verified
                        items["comment"] = comment
                        items["voting"] = voting
                        items["review_images"] = review_images
                        items["ASIN"] = ASIN
                        items["date_scraped"] = date_scraped

                        yield items

                # next page url
                next_page_partial_url = response.xpath('//li[@class="a-last"]/a/@href').extract_first()

                if next_page_partial_url and not reached_old_reviews:
                    # remove name of product in front
                    partial_url = str(next_page_partial_url).split("/product-reviews", 1)[1]
                    # concat partial_url with base url for product reviews
                    next_page_url = "https://www.amazon.com/product-reviews" + str(partial_url)

                    # continue following the next page link as long as there is content to scrape in the next page
                    yield scrapy.Request(next_page_url, callback=self.parse)
                    # introduce random delay between requests to reduce risk of being blocked
                    time.sleep(random.randint(4, 8))
                
                elif next_page_partial_url and reached_old_reviews:
                    print("Reached the point of old reviews. Stop scraping for current product review")
                    #print("loaded reviews data", reviews_data)
                    reviews_data[ASIN] = date_scraped
                    #print("updated reviews data", reviews_data)
                    with open(self.tracker_path, 'w') as reviews_file:
                        json.dump(reviews_data, reviews_file)
                    raise CloseSpider('termination condition met')              
                    
                else:
                    print("No more next review page button. Stop scraping for current product review")
                    reviews_data[ASIN] = date_scraped
                    with open(self.tracker_path, 'w') as reviews_file:
                        json.dump(reviews_data, reviews_file)
                    raise CloseSpider('termination condition met')     

