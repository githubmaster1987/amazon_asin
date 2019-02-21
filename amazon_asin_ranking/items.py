# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonAsinRankingItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    search_key = scrapy.Field()
    asin = scrapy.Field()
    ranking = scrapy.Field()
    scraped_date = scrapy.Field()