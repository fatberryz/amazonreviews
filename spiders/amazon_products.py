# -*- coding: utf-8 -*-
# Importing Scrapy Library
import json
import re
import random
import time

import js2xml
import pandas as pd
import scrapy
from amazonreviews.items import AmazonProductsItem
from js2xml.utils.vars import get_vars
from random_user_agent.params import OperatingSystem, SoftwareName
from random_user_agent.user_agent import UserAgent
from scrapy import signals


# Creating a new class to implement Spider
class AmazonReviewsSpider(scrapy.Spider):
    # Spider name
    name = 'amazon_products'
    custom_settings = {}

    def __init__(self, *args, **kwargs):
        super(AmazonReviewsSpider, self).__init__(*args, **kwargs)
        config = kwargs['config'].split(',')
        self.log_output = config[1]
        mode = config[4]
        start_urls = []
        if mode == "main":
            product_df_path = config[0]
            start_row = int(config[2])
            end_row = int(config[3])

            product_df = pd.read_csv(product_df_path)

            for product_url in product_df['url'].iloc[start_row:end_row]:
                start_urls.append(product_url)

        if mode == "outstanding":
            outstanding_df = pd.read_csv(self.log_output)
            outstanding_df = outstanding_df[outstanding_df['scraped'].astype(int) == 0]
            for raw_url in outstanding_df['url']:
                format_url = raw_url.replace("start_requests/item_scraped_count/", "")
                start_urls.append(format_url)

        self.start_urls = start_urls
        self.logger.info(self.start_urls)

    # Domain names to scrape
    allowed_domains = ['amazon.co.uk', 'amazon.com']

    # Generates user agent randomly
    software_names = [SoftwareName.CHROME.value]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
    user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)
    user_agent = user_agent_rotator.get_random_user_agent()

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
                if (prefix + url in stats and stats[prefix + url] < 1) or (prefix + url not in stats):
                    log_file.write(prefix + url + ",0" + ",0" + '\n')

    # Defining a Scrapy parser
    def parse(self, response):
        items = AmazonProductsItem()

        description = ''
        price = ''
        rating = ''
        availability = ''

        retries = 1
        while retries <= 3 and description == '':
            if description == '':
                description_raw = response.xpath("//div[contains(@id,'featurebullets_feature_div') or contains(@id,'feature-bullets')]//span[@class='a-list-item']//text()").getall()
                description = []
                for description_temp in description_raw:
                    description.append(description_temp.strip())

                description = ' '.join(description).strip()

            if rating == '':
                rating = ''.join(response.xpath("//span[contains(@data-hook,'arp-rating-out-of-text') or contains(@data-hook,'rating-out-of-text')]//text()").extract()).strip()
            
            if price == '':
                price = ''.join(response.xpath('//span[contains(@id,"priceblock_ourprice") or contains(@id,"ourprice") or contains(@id,"saleprice")]//text()').extract()).strip()

            if availability == '':
                availability = ''.join(response.xpath('//div[@id="availability"]//span[@class="a-size-medium a-color-price"]//text()').extract()).strip()
            
            wait_time = random.randint(4, 8)
            print(f"Current retries at {retries}/3. Will pause for {wait_time} seconds before scrapping.")
            retries += 1
            time.sleep(wait_time)

            ASIN = response.request.url.split('/')[4]

        # Combining the results
        items["ASIN"] = ASIN
        items["description"] = description
        items["price"] = price
        items["rating"] = rating
        items["availability"] = availability

        yield items
