# -*- coding: utf-8 -*-
import scrapy
import time
import re
from random import randint
import base64
import csv
import os
import json
import os.path
import re
from . import proxylist
from . import useragent
from amazon_asin_ranking.items import AmazonAsinRankingItem
from io import StringIO
from datetime import datetime
from datetime import date
from scrapy.http import Request, FormRequest
from .models import model
from .mysql_manage import *


class AmazonSpider(scrapy.Spider):
    name = 'amazon'
    allowed_domains = ['amazon.com']

    selected_category_index = 0
    categories = [
        'Book',
    ]

    category_root_urls = [
        'https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Dstripbooks&field-keywords=',
    ]

    headers = {
        "Host": "www.amazon.com",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
    }

    proxy_lists = proxylist.proxies
    useragent_lists = useragent.user_agent_list
    queue_list = []
    db_total_count = 0
    db_scraped_count = 0
    starttime = datetime.now()

    def set_proxies(self, url, callback, headers=None, proxy_url=None):
        if headers:
            req = Request(url=url, callback=callback,
                          dont_filter=True, headers=headers, cookies={})
        else:
            req = Request(url=url, callback=callback, dont_filter=True)

        # if proxy_url is not None:
        proxy_url = proxylist.get_proxy()
        # print("++++++++++++++++++++++++")
        # print("Proxy", proxy_url)

        user_pass = base64.encodestring('{}:{}'.format(
            proxylist.proxy_username, proxylist.proxy_password).encode()).strip().decode('utf-8')
        req.meta['proxy'] = "http://" + proxy_url
        req.headers['Proxy-Authorization'] = 'Basic ' + user_pass

        # user_agent = random.choice(self.useragent_lists)
        # req.headers['User-Agent'] = user_agent
        return req

    def __init__(self, category_index=-1, instance_index=0, instance_count=10, *args, **kwargs):
        super(AmazonSpider, self).__init__(*args, **kwargs)
        self.selected_category_index = int(category_index)
        self.instance_count = int(instance_count)
        self.instance_index = int(instance_index)

    def start_requests(self):
        self.starttime = datetime.now()

        if self.selected_category_index != -1:
            print(" -> Selected Category :",
                  self.categories[self.selected_category_index])
            print(" -> Selected Category URL :",
                  self.category_root_urls[self.selected_category_index])

            req = self.set_proxies(
                self.category_root_urls[self.selected_category_index],
                self.parse_root_category, self.headers)
            yield req
        else:
            db_listing = db.session.query(model.CategoryURL).all()
            total_len = len(db_listing)

            print(" -> Total DB Category :", total_len)

            url_list = []
            self.db_total_count = 0
            for i, db_item in enumerate(db_listing):
                if (i > self.instance_index * total_len / self.instance_count) and (i < (self.instance_index + 1) * total_len / self.instance_count):
                    url_list.append(db_item.url)
                    self.db_total_count += db_item.total

            print(" -> URL :", len(url_list), ", Total :", self.db_total_count)

            for url in url_list:
                print(' ->', url)
                req = self.set_proxies(
                    url,
                    self.parse_listing, self.headers)
                yield req

    def parse_root_category(self, response):
        menu_lists = response.xpath(
            '//ul[contains(@class, "a-unordered-list a-nostyle a-vertical s-ref-indent-one")]//li/span/a')

        for obj in menu_lists:
            # title = obj.xpath("span/text()").extract_first("")
            link = response.urljoin(obj.xpath("@href").extract_first(""))

            # print(title, link)
            req = self.set_proxies(
                link,
                self.parse_second_category, self.headers)

            yield req

    def parse_second_category(self, response):
        menu_lists = response.xpath(
            '//ul[contains(@class, "a-unordered-list a-nostyle a-vertical s-ref-indent-two")]//li/span/a')

        if len(menu_lists) == 0:
            total_count_str = response.xpath(
                '//span[@id="s-result-count"]/text()').extract_first()

            breadcrumbs = ':'.join(response.xpath(
                '//span[@id="s-result-count"]//a/text()').extract())

            total_count = None
            try:
                total_count = re.search(
                    "\s([\d,]+)\sresults", total_count_str, re.I | re.S | re.M).group(1)

                total_count = total_count.replace(",", '')
            except Exception as e:
                print('========================================')
                print(total_count_str)
                print(e)
                with open("response.txt", 'w') as f:
                    f.write(total_count_str)

                with open("response.html", 'w') as f:
                    f.write(response.text)

                return

            obj = {'category': self.categories[self.selected_category_index],
                   'url': response.meta['link'],
                   "status": 0,
                   'total': int(total_count),
                   'subCategory': breadcrumbs + ':' + response.meta['title']
                   }
            # print(obj)

            self.queue_list.append(obj)

            deltatime = datetime.now() - self.starttime
            print(' -----------> Time:', deltatime.__str__())
            print(len(self.queue_list))

            if len(self.queue_list) > randint(30, 50):
                print('------------------------> db saved',
                      self.queue_list[0]["subCategory"])
                values = []
                for d in self.queue_list:
                    values.append('("{}", "{}", {}, {}, "{}")'.format(
                        d["category"], d["url"], d["status"], d["total"], d["subCategory"]))

                sql_query = 'INSERT INTO `CategoryURL`(`category`, `url`, \
                        `status`, `total`, `subCategory`) VALUES {} \
                        ON DUPLICATE KEY UPDATE category=VALUES(category), \
                        subCategory=VALUES(subCategory), status=VALUES(status), url=VALUES(url), total=VALUES(total)'.format(', '.join(values))

                # print(sql_query)
                try:
                    db.session.execute(sql_query)
                    db.session.commit()
                except Exception as e:
                    db.session.rollback()
                    print(e)

                self.queue_list = []

            print(' -> Total: ', total_count)

        for obj in menu_lists:
            title = obj.xpath("span/text()").extract_first("")
            link = response.urljoin(obj.xpath("@href").extract_first(""))

            # print('-------------------------->')
            # print(title, link)
            req = self.set_proxies(
                link,
                self.parse_second_category, self.headers)
            req.meta['title'] = title
            req.meta['link'] = link
            yield req

    def parse_listing(self, response):
        links = response.xpath(
            "//a[contains(@class, 's-color-twister-title-link')]|//div[contains(@class, 's-result-item')]//h5/a[contains(@class, 'a-link-normal')]")
        print("LEN = ", len(links))

        if len(links) == 0:
            return

        for link_item in links:
            link = response.urljoin(link_item.xpath('@href').extract_first())
            req = self.set_proxies(
                link,
                self.parse_detail_page, self.headers)

            yield req

        nextPage = response.xpath(
            "//a[@id='pagnNextLink']|li[@class='a-last']/a")
        if nextPage:
            nextPageLink = response.urljoin(
                nextPage.xpath('@href').extract_first())
            print('--------------->', nextPageLink)

            self.headers["Refer"] = response.url

            req = self.set_proxies(
                nextPageLink, self.parse_listing, self.headers)
            yield req

    def parse_detail_page(self, response):
        isbn_10 = response.xpath(
            '//div[@class="content"]/ul/li/b[contains(text(), "ISBN-10:")]/../text()').extract_first()

        if isbn_10 is None:
            isbn_10 = ''

        asin = response.xpath(
            '//div[@class="content"]/ul/li/b[contains(text(), "ASIN:")]/../text()').extract_first()

        if asin is None:
            asin = ''

        rankings = response.xpath('//li[@id="SalesRank"]/text()')

        ranking = None
        for ranking_item in rankings.extract():
            ranking_str = ranking_item.strip()

            if 'in Books' in ranking_str:
                ranking = re.search(
                    "#([\d,]+)\sin", ranking_str, re.I | re.S | re.M).group(1)
                ranking = ranking.replace(",", '')
                break
            elif 'Paid in' in ranking_str:
                # print('+++++++++++++++++++++++', ranking_str)
                link = response.urljoin(response.xpath(
                    '//div[@id="MediaMatrix"]//li[contains(@class, "swatchElement")]//a/span[contains(text(), "Paperback")]/../@href|//div[@id="MediaMatrix"]//li[contains(@class, "swatchElement")]//a/span[contains(text(), "Hardcover")]/../@href').extract_first())

                req = self.set_proxies(
                    link,
                    self.parse_detail_page, self.headers)

                yield req

                return

        obj = {'asin': asin, 'isbn_10': isbn_10, "ranking": ranking}
        # print(obj)

        self.queue_list.append(obj)
        self.db_scraped_count += 1
        deltatime = datetime.now() - self.starttime

        print(' -----------> Time:', deltatime.__str__())
        print(' -----------> DB Total: ', self.db_total_count, ' ---------> Scraped:', self.db_scraped_count)

        if len(self.queue_list) > randint(30, 100):
            print('------------------------> db saved',
                  self.queue_list[0]["asin"], self.queue_list[0]["isbn_10"])
            values = []
            for d in self.queue_list:
                values.append('("{}", "{}", "{}")'.format(
                    d["asin"], d["isbn_10"], d["ranking"]))

            sql_query = 'INSERT INTO `Listing`(`asin`, `isbn10`, `ranking`) VALUES {} ON DUPLICATE KEY UPDATE asin=VALUES(asin), isbn10=VALUES(isbn10), ranking=VALUES(ranking)'.format(
                ', '.join(values))
            try:
                db.session.execute(sql_query)
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                print(e)

            self.queue_list = []
