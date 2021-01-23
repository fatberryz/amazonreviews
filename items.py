# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonReviewsItem(scrapy.Item):
    # define the fields for your item here like:
    stars = scrapy.Field()
    profile_name = scrapy.Field()
    profile_link = scrapy.Field()
    profile_image = scrapy.Field()
    title = scrapy.Field()
    date = scrapy.Field()
    style = scrapy.Field()
    verified = scrapy.Field()
    comment = scrapy.Field()
    voting = scrapy.Field()
    review_images = scrapy.Field()
    ASIN = scrapy.Field()


class AmazonProfilesItem(scrapy.Item):
    # define the fields for your item here like:
    json_data = scrapy.Field()
    acc_num = scrapy.Field()
    name = scrapy.Field()
    occupation = scrapy.Field()
    location = scrapy.Field()
    description = scrapy.Field()
    badges = scrapy.Field()
    ranking = scrapy.Field()


class AmazonProductsItem(scrapy.Item):
    # define the fields for your item here like:
    ASIN = scrapy.Field()
    description = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    availability = scrapy.Field()
