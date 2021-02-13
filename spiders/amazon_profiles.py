# -*- coding: utf-8 -*-
# Importing Scrapy Library
import json
import platform
import re
import time
from datetime import datetime

import pandas as pd

import js2xml
import scrapy
from js2xml.utils.vars import get_vars
from random_user_agent.params import OperatingSystem, SoftwareName
from random_user_agent.user_agent import UserAgent
from scrapy import signals

# To allow Mac to load spider module from parent folder
if platform.system() == "Darwin" or platform.system() == "Linux":
    import os, sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from items import AmazonProfilesItem
else:
    from amazonreviews.items import AmazonProfilesItem


# Creating a new class to implement Spider
class AmazonReviewsSpider(scrapy.Spider):
    # Spider name
    name = 'amazon_profiles'
    custom_settings = {}

    def __init__(self, *args, **kwargs):
        super(AmazonReviewsSpider, self).__init__(*args, **kwargs)
        config = kwargs['config'].split(',')
        self.log_output = config[1]
        mode = config[4]
        start_urls = []

        if mode == "main":
            profile_df_path = config[0]
            start_row = int(config[2])
            end_row = int(config[3])
            profiles_df = pd.read_csv(profile_df_path)

            for profile_url in profiles_df['url'].iloc[start_row:end_row]:
                start_urls.append(profile_url)

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
        items = AmazonProfilesItem()
        date_scraped = datetime.today().strftime('%Y-%m-%d')

        if response.status == 404:
            json_data = ''
            acc_num = response.url.split('amzn1.account.')[-1].split("/")[0]
            name = ''
            occupation = ''
            location = ''
            description = ''
            badges = ''
            ranking = ''


            items["json_data"] = json_data
            items["acc_num"] = acc_num
            items["name"] = name
            items["occupation"] = occupation
            items["location"] = location
            items["description"] = description
            items["badges"] = badges
            items["ranking"] = ranking
            items["date_scraped"] = date_scraped

            yield items
        try:
            account_num = response.url.split('amzn1.account.')[-1].split("/")[0]
            # print("account_num", account_num)
            pattern = r"window.CustomerProfileRootProps = {([^}]*)}"
            # print("pattern", pattern)
            token_pattern = r'"token":"((\\"|[^"])*)"'
            # print("response", response)
            # print("response xpath script text", response.xpath("//script//text()"))
            # print("response xpath", response.xpath("//script//text()").re(pattern))
            reviews = response.xpath("//script//text()").re(pattern)[0]
            token = re.search(token_pattern, reviews).group(1)

            summary = response.xpath('//script[contains(., " ")]//text()').extract_first()
            summary = get_vars(js2xml.parse(summary))

            # helpful votes etc.
            meta = {
                "name": summary['window.CustomerProfileRootProps']['nameHeaderData']['name'],
                "occupation": summary['window.CustomerProfileRootProps']['bioData']['occupation'],
                "location": summary['window.CustomerProfileRootProps']['bioData']['location'],
                "description": summary['window.CustomerProfileRootProps']['bioData']['personalDescription'],
                "badges": summary['window.CustomerProfileRootProps']['bioData']['badges']['summary'],
                "ranking": summary['window.CustomerProfileRootProps']['bioData']['topReviewerInfo']['rank'],
                "acc_num": account_num
            }

            # Getting the user profile response by requesting with obtained token
            next_url = "https://www.amazon.com/profilewidget/timeline/visitor?nextPageToken=&filteredContributionTypes" \
                       "=productreview%2Cglimpse%2Cideas&directedId=amzn1.account.{acc_num}&token={token}" \
                .format(acc_num=account_num, token=token)

            yield scrapy.FormRequest(next_url, callback=self.parse_profile, meta=meta)
            # introduce random delay between requests to reduce risk of being blocked
            time.sleep(random.randint(4, 8))

        except ValueError as e:
            if str(e) == "All strings must be XML compatible: Unicode or ASCII, no NULL bytes or control characters":
                # This exception occurs when we cant find any reviews on the profile (maybe deleted)
                json_data = ''
                acc_num = response.url.split('amzn1.account.')[-1].split("/")[0]
                name = ''
                occupation = ''
                location = ''
                description = ''
                badges = ''
                ranking = ''

                items["json_data"] = json_data
                items["acc_num"] = acc_num
                items["name"] = name
                items["occupation"] = occupation
                items["location"] = location
                items["description"] = description
                items["badges"] = badges
                items["ranking"] = ranking
                items["date_scraped"] = date_scraped

                yield items
            else:
                pass

    def parse_profile(self, response):
        items = AmazonProfilesItem()
        date_scraped = datetime.today().strftime('%Y-%m-%d')

        try:
            data = json.loads(response.body)
            json_data = data
            acc_num = response.meta['acc_num']
            name = response.meta['name']
            occupation = response.meta['occupation']
            location = response.meta['location']
            description = response.meta['description']
            badges = response.meta['badges']
            ranking = response.meta['ranking']

            items["json_data"] = json_data
            items["acc_num"] = acc_num
            items["name"] = name
            items["occupation"] = occupation
            items["location"] = location
            items["description"] = description
            items["badges"] = badges
            items["ranking"] = ranking
            items["date_scraped"] = date_scraped

            yield items

        except:
            pass
