# -*- coding: utf-8 -*-
# Importing Scrapy Library
import scrapy
from amazonreviews.items import AmazonReviewsItem
from scrapy import signals
import pandas as pd


# Creating a new class to implement Spider
class AmazonReviewsSpider(scrapy.Spider):
    # Spider name
    name = 'amazon_reviews'
    custom_settings = {}

    def __init__(self, *args, **kwargs):
        super(AmazonReviewsSpider, self).__init__(*args, **kwargs)

        config = kwargs['config'].split(',')
        self.log_output = config[3]
        mode = config[4]
        start_urls = []

        if mode == "outstanding":
            outstanding_df = pd.read_csv(self.log_output)
            outstanding_df = outstanding_df[outstanding_df['scraped'].astype(int) == 0]
            for raw_url in outstanding_df['url']:
                format_url = raw_url.replace("start_requests/item_scraped_count/", "")
                start_urls.append(format_url)
            self.start_urls = start_urls

        if mode == "main":
            url_name = config[0]
            start_page = int(config[1])
            end_page = int(config[2])
            if len(url_name) > 0:
                for i in range(start_page, end_page):
                    start_urls.append(url_name + str(i))
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
                    log_file.write(prefix+url + "," + str(stats[prefix + url]) + '0' + '\n')
                if prefix + url not in stats:
                    log_file.write(prefix + url + ",0" + ",0" + '\n')

    # Defining a Scrapy parser
    def parse(self, response):
        items = AmazonReviewsItem()

        data = response.css('#cm_cr-review_list')
        # Collecting user reviews
        print(response.request.url)
        reviews = data.css('div[data-hook="review"]')

        # Combining the results
        for review in reviews:
            if ''.join(review.xpath('.//i[@data-hook="review-star-rating"]//text()').extract()).strip() != '':
                stars = ''.join(review.xpath('.//i[@data-hook="review-star-rating"]//text()').extract()).strip()
            else: 
                stars = ''.join(review.xpath('.//i[@data-hook="cmps-review-star-rating"]//text()').extract()).strip()

            profile_name =  ''.join(review.xpath('.//span[@class="a-profile-name"]//text()').extract()).strip()
            profile_link = ''.join(review.xpath('.//div[@data-hook="genome-widget"]//a/@href').extract()).strip()
            profile_image =  ''.join(review.xpath('.//div[@class="a-profile-avatar"]//img/@data-src').extract()).strip()
            title = ''.join(review.xpath('.//a[@data-hook="review-title"]//text()').extract()).strip()
            date =  ''.join(review.xpath('.//span[@data-hook="review-date"]//text()').extract()).strip()
            style =  ''.join(review.xpath('.//a[@data-hook="format-strip"]//text()').extract()).strip()
            verified = ''.join(review.xpath('.//span[@data-hook="avp-badge"]//text()').extract()).strip()
            comment =  ''.join(review.xpath('.//span[@data-hook="review-body"]//text()').extract()).strip()
            voting = ''.join(review.xpath('.//span[@data-hook="review-voting-widget"]//text()').extract()).strip()
            review_images = len(review.xpath('.//div[@class="review-image-tile-section"]//img'))
            ASIN = response.request.url.split('/')[5]

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

            yield items


