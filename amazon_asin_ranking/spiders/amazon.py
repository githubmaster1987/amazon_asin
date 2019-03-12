# -*- coding: utf-8 -*-
import scrapy
import time
import re
from random import randint, choice
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
        'CD',
        'DVD'
    ]

    category_root_urls = [
        'https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Dstripbooks&field-keywords=',
        'https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Dpopular&field-keywords=',
        'https://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Dmovies-tv&field-keywords='
    ]

    headers = {
        "Host": "www.amazon.com",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
    }

    CATEGORY_BOOK = 0
    CATEGORY_CD = 1
    CATEGORY_DVD = 2

    url_attach = "&rh=p_n_format_browse-bin:2650304011|2650305011|2650307011|2650308011"

    MAX_RANKING_BOOK = 2000000
    MAX_RANKING_CD_DVD = 280000

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

        proxy_url = proxylist.get_proxy()
        user_pass = base64.encodestring('{}:{}'.format(
            proxylist.proxy_username, proxylist.proxy_password).encode()).strip().decode('utf-8')
        # req.meta['proxy'] = "http://" + proxy_url
        # req.headers['Proxy-Authorization'] = 'Basic ' + user_pass

        user_agent = choice(self.useragent_lists)
        # req.headers['User-Agent'] = user_agent
        return req

    def __init__(self, category_index=0, instance_index=0, instance_count=10, is_category=0, *args, **kwargs):
        super(AmazonSpider, self).__init__(*args, **kwargs)
        self.selected_category_index = int(category_index)
        self.instance_count = int(instance_count)
        self.instance_index = int(instance_index)
        self.is_category = int(is_category)

    def start_requests(self):
        # url = "https://www.amazon.com/Iron-Man-Movie-Collection-Blu-ray/dp/B00FFBA87E/ref=sr_1_36?fst=as%3Aoff&qid=1552360042&refinements=p_n_theme_browse-bin%3A2650363011%2Cp_n_format_browse-bin%3A2650304011%7C2650305011%7C2650307011&rnid=2650303011&s=movies-tv&sr=1-36"
        # req = self.set_proxies(
        #         url,
        #         self.parse_detail_page, self.headers)
        # yield req
        # return

        # url = "https://www.amazon.com/action-adventure-dvd-bluray/b/ref=MoviesHPBB_Genres_Action?ie=UTF8&node=2650363011&pf_rd_m=ATVPDKIKX0DER&pf_rd_s=merchandised-search-left-2&pf_rd_r=MW0YJFW2D2W4RPM07G4X&pf_rd_r=MW0YJFW2D2W4RPM07G4X&pf_rd_t=101&pf_rd_p=d175e841-d695-46c7-a5ed-72ccc2f58f7d&pf_rd_p=d175e841-d695-46c7-a5ed-72ccc2f58f7d&pf_rd_i=2921756011" + self.url_attach
        # req = self.set_proxies(
        #     url,
        #     self.parse_listing, self.headers)
        # yield req
        # return

        processname = "scrapy crawl amazon -a category_index={} -a instance_index={} -a instance_count={}".format(self.selected_category_index, self.instance_index, self.instance_count)

        if self.is_category == 1:
            processname = "scrapy crawl amazon -a category_index={} -a is_category=1".format(self.selected_category_index)

        print('++++++++++++++++++++++++++')
        print(processname)

        tmp = os.popen("ps -Af").read()

        proccount = tmp.count(processname)

        if proccount > 1:
            print(proccount, ' processes running of ', processname, 'type')
            print('+++++++++++++++++++++++++++++++++++++++++++++++++')
            return

        self.starttime = datetime.now()

        if self.is_category == 1:
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
            db_listing = db.session.query(model.CategoryURL).filter(
                model.CategoryURL.category == self.categories[self.selected_category_index]).all()
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
                if self.selected_category_index == self.CATEGORY_DVD:
                    url = url + self.url_attach
                print(' ->', url)
                req = self.set_proxies(
                    url,
                    self.parse_listing, self.headers)
                yield req

    def parse_root_category(self, response):
        root_menu_xpath = '//ul[contains(@class, "a-unordered-list a-nostyle a-vertical s-ref-indent-one")]//li/span/a'

        if self.selected_category_index == self.CATEGORY_CD:
            root_menu_xpath = '//h3[contains(text(), "Browse by Genre")]/following-sibling::ul[1]/li/a'
        elif self.selected_category_index == self.CATEGORY_DVD:
            root_menu_xpath = '//h3[contains(text(), "Popular Genres")]/following-sibling::ul[1]/li/a'
        menu_lists = response.xpath(
            root_menu_xpath)

        for obj in menu_lists:
            title = obj.xpath("span/text()").extract_first("")

            if (self.selected_category_index == self.CATEGORY_DVD) or (self.selected_category_index == self.CATEGORY_CD):
                title = obj.xpath("text()").extract_first("")

            link = response.urljoin(obj.xpath("@href").extract_first("")) + self.url_attach
            print('---------------------------> ', title, link)
            req = self.set_proxies(
                link,
                self.parse_second_category, self.headers)

            if (self.selected_category_index == self.CATEGORY_DVD) or (self.selected_category_index == self.CATEGORY_CD):
                req.meta["title"] = title
                req.meta["link"] = link

            yield req

    def parse_second_category(self, response):
        menu_lists = response.xpath(
            '//ul[contains(@class, "a-unordered-list a-nostyle a-vertical s-ref-indent-two")]//li/span/a')

        captcha = response.xpath(
            '//form[@action="/errors/validateCaptcha"]')

        if len(captcha) > 0:
            print("////////////////////////////////////////////")
            print(captcha)
            print(response.meta)
            print(response.url)

            with open("captcha.html", 'w') as f:
                f.write(response.text)

            req = self.set_proxies(
                response.url,
                self.parse_second_category, self.headers)

            if 'title' in response.meta.keys():
                req.meta['title'] = response.meta['title']

            if 'link' in response.meta.keys():
                req.meta['link'] = response.meta['link']

            yield req
            return

        if len(menu_lists) == 0:
            total_count_str = response.xpath(
                '//span[@id="s-result-count"]/text()').extract_first()

            breadcrumbs = ':'.join(response.xpath(
                '//span[@id="s-result-count"]//a/text()').extract()).strip()

            print('???????????????????? RESPONSE ?????????????????????')
            print(response.meta)

            if total_count_str is None:
                total_count_str = ''.join(response.xpath(
                    '//span[@data-component-type="s-result-info-bar"]//div[not(@class="right")]/span/text()').extract()).strip()
                print('******************* Small Total *******************')
                print(total_count_str)

            if (total_count_str is None) or (total_count_str == ""):
                print('================= total is none =======================')
                print(response.url)
                with open("response.html", 'w') as f:
                    f.write(response.text)

                # req = self.set_proxies(
                # response.url,
                #     self.parse_second_category, self.headers)

                # if 'title' in response.meta.keys():
                #     req.meta['title'] = response.meta['title']

                # if 'link' in response.meta.keys():
                #     req.meta['link'] = response.meta['link']

                # yield req

                return

            total_count = None
            try:
                total_count = re.search(
                    "([\d,]+)\sresults", total_count_str, re.I | re.S | re.M).group(1)

                total_count = total_count.replace(",", '')
            except Exception as e:
                print('================== total count exception ======================')
                print(response.url)
                print(total_count_str)
                print(e)
                with open("response.txt", 'w') as f:
                    f.write(total_count_str)

                return

            if 'link' in response.meta.keys():
                category_url = response.meta['link']
            else:
                category_url = response.url

            if 'title' in response.meta.keys():
                category_title = response.meta['title']
            else:
                print('================ dvd title is none ========================')
                print(response.url)
                with open("title.html", 'w') as f:
                    f.write(response.text)

                return

            if category_title == "":
                print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                print(response.url)
                with open("title.html", 'w') as f:
                    f.write(response.text)

                return

            subCategory_str = breadcrumbs + ':' + category_title

            if (self.selected_category_index == self.CATEGORY_DVD) or (self.selected_category_index == self.CATEGORY_CD):
                subCategory_str = response.xpath('//title/text()').extract_first()

            obj = {'category': self.categories[self.selected_category_index],
                   'url': category_url,
                   "status": 0,
                   'total': int(total_count),
                   'subCategory': subCategory_str
                   }
            print(obj)

            self.queue_list.append(obj)

            deltatime = datetime.now() - self.starttime
            print(' -----------> Time:', deltatime.__str__())
            print(len(self.queue_list))

            # if len(self.queue_list) > randint(30, 50):
            if len(self.queue_list) > 0:
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

                    print('------------------------> db saved',
                          self.queue_list[0]["subCategory"])
                except Exception as e:
                    print('\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\')
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
            "//a[contains(@class, 's-color-twister-title-link')]|//div[contains(@class, 's-result-item')]//a[@title='product-detail']|//div[contains(@class, 's-result-item')]//h5/a[contains(@class, 'a-link-normal')]|//div[contains(@id, 'resultItems')]//a[contains(@class, 'a-link-normal')]")
        print("LEN = ", len(links))

        captcha = response.xpath(
            '//form[@action="/errors/validateCaptcha"]')

        if len(captcha) > 0:
            print('/////////////////////////////')
            print(captcha)
            print(response.url)
            req = self.set_proxies(
                response.url,
                self.parse_listing, self.headers)

            return

        if len(links) == 0:
            print('??????????????????????????????')
            print(response.url)

            with open("link.html", 'w') as f:
                f.write(response.text)

            return

        for link_item in links:
            link = response.urljoin(link_item.xpath('@href').extract_first())
            req = self.set_proxies(
                link,
                self.parse_detail_page, self.headers)

            yield req

        nextPage = response.xpath(
            "//a[@id='pagnNextLink']|//li[@class='a-last']/a")
        if nextPage:
            nextPageLink = response.urljoin(
                nextPage.xpath('@href').extract_first())

            if self.selected_category_index == self.CATEGORY_DVD:
                nextPageLink = nextPageLink + self.url_attach
            print('--------------->', nextPageLink)

            self.headers["Refer"] = response.url

            req = self.set_proxies(
                nextPageLink, self.parse_listing, self.headers)
            yield req

    def parse_detail_page(self, response):
        # print('??????????????????????????????? Detail ?????????????????????????????')
        # print(response.url)
        captcha = response.xpath(
            '//form[@action="/errors/validateCaptcha"]')

        if len(captcha) > 0:
            print('/////////////////////////////')
            print(captcha)
            print(response.url)
            req = self.set_proxies(
                response.url,
                self.parse_detail_page, self.headers)

            return

        isbn_10 = response.xpath(
            '//div[@class="content"]/ul/li/b[contains(text(), "ISBN-10:")]/../text()|//table[contains(@id, "productDetails_techSpec")]//th[contains(text(), "ISBN-10")]/../td/text()').extract_first()

        if isbn_10 is None:
            isbn_10 = ''

        asin = response.xpath(
            '//div[@class="content"]/ul/li/b[contains(text(), "ASIN:")]/../text()|//table[contains(@id, "productDetails_techSpec")]//th[contains(text(), "ASIN")]/../td/text()').extract_first()

        if asin is None:
            asin = ''

        if (asin == '') and (isbn_10 == ''):
            print('+++++++++++++++++++++++++++++++')
            print(response.url)
            print('-----Asin----', asin, isbn_10)

            with open("empty.html", 'w') as f:
                f.write(response.text)

        td_ranking = response.xpath(
            '//table[contains(@id, "productDetails_techSpec")]//th[contains(text(), "Best Sellers Rank")]/../td/text()').extract_first()

        # print('??????????????????????????????? Asin ?????????????????????????????')
        # print(asin, td_ranking)

        if td_ranking is not None:
            ranking = td_ranking.replace(",", '')
        else:
            rankings = response.xpath('//li[@id="SalesRank"]/text()')

            ranking_list = []
            ranking = None
            for ranking_item in rankings.extract():
                ranking_str = ranking_item.strip()
                ranking_list.append(ranking_str)

                ranking_category_string = 'in Books'

                if self.selected_category_index == self.CATEGORY_CD:
                    ranking_category_string = 'in CDs & Vinyl'
                elif self.selected_category_index == self.CATEGORY_DVD:
                    ranking_category_string = 'in Movies & TV'

                print(ranking_str, ranking_category_string)
                if ranking_category_string in ranking_str:
                    ranking = re.search(
                        "#([\d,]+)\sin", ranking_str, re.I | re.S | re.M).group(1)
                    ranking = ranking.replace(",", '')
                    break
                elif 'Paid in' in ranking_str:
                    # print('+++++++++++++++++++++++', ranking_str)
                    if self.selected_category_index == self.CATEGORY_BOOK:
                        link = response.urljoin(response.xpath(
                            '//div[@id="MediaMatrix"]//li[contains(@class, "swatchElement")]//a/span[contains(text(), "Paperback")]/../@href|//div[@id="MediaMatrix"]//li[contains(@class, "swatchElement")]//a/span[contains(text(), "Hardcover")]/../@href').extract_first())
                    elif self.selected_category_index == self.CATEGORY_CD:
                        link = response.urljoin(response.xpath(
                            '//div[@id="MediaMatrix"]//li[contains(@class, "swatchElement")]//a/span[contains(text(), "Audio CD")]/../@href|//div[@id="MediaMatrix"]//li[contains(@class, "swatchElement")]//a/span[contains(text(), "Vinyl") and not(contains(text(), "Vinyl Bound"))]/../@href').extract_first())
                    elif self.selected_category_index == self.CATEGORY_DVD:
                        link = response.urljoin(response.xpath(
                            '//div[@id="MediaMatrix"]//li[contains(@class, "swatchElement")]//a/span[contains(text(), "Multi-Format")]/../@href|//div[@id="MediaMatrix"]//li[contains(@class, "swatchElement")]//a/span[contains(text(), "3D")]/../@href|//div[@id="MediaMatrix"]//li[contains(@class, "swatchElement")]//a/span[contains(text(), "DVD")]/../@href').extract_first())

                    req = self.set_proxies(
                        link,
                        self.parse_detail_page, self.headers)

                    yield req

                    return

            if (ranking is None) or (ranking == ""):
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
                print(response.url)
                print(ranking_list)

                with open("empty1.html", 'w') as f:
                    f.write(response.text)
                return

        obj = {'asin': asin.strip(), 'isbn_10': isbn_10.strip(),
               "ranking": ranking.strip()}

        if (obj["asin"] == "") and (obj["isbn_10"] == ""):
            print('>>>>>>>>>>>>>>>>>>>>> Empty >>>>>>>>>>>>>>>>>>>>')
            print(obj)
            return

        if self.selected_category_index == self.CATEGORY_BOOK:
            if (int(obj["ranking"]) >= self.MAX_RANKING_BOOK):
                print('???????????????? MAX RANKING REACHED ????????????????', obj["ranking"])
                return
        elif (self.selected_category_index == self.CATEGORY_DVD) or (self.selected_category_index == self.CATEGORY_CD):
            if (int(obj["ranking"]) >= self.MAX_RANKING_CD_DVD):
                print('???????????????? MAX RANKING REACHED ????????????????', obj["ranking"])
                return

        self.queue_list.append(obj)
        self.db_scraped_count += 1
        deltatime = datetime.now() - self.starttime

        print(' -----------> Time:', deltatime.__str__())
        print(' -----------> Obj:', obj["ranking"], obj["asin"], self.selected_category_index)
        print(' -----------> DB Total: ', self.db_total_count,
              ' ---------> Scraped:', self.db_scraped_count)

        if len(self.queue_list) > randint(30, 100):
        # if len(self.queue_list) > 0:
            print('------------------------> db saved',
                  self.queue_list[0]["asin"], self.queue_list[0]["isbn_10"], self.queue_list[0]["ranking"])
            values = []
            for d in self.queue_list:
                values.append('("{}", "{}", "{}")'.format(
                    d["asin"], d["isbn_10"], d["ranking"]))

            table_name = "Listing"

            if self.selected_category_index == self.CATEGORY_CD:
                table_name = "ListingCD"
            elif self.selected_category_index == self.CATEGORY_DVD:
                table_name = "ListingDVD"

            sql_query = 'INSERT INTO `{}`(`asin`, `isbn10`, `ranking`) VALUES {} ON DUPLICATE KEY UPDATE asin=VALUES(asin), isbn10=VALUES(isbn10), ranking=VALUES(ranking)'.format(table_name,
                                                                                                                                                                                   ', '.join(values))
            try:
                db.session.execute(sql_query)
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                print(e)

            self.queue_list = []


# https://www.amazon.com/Toy-Story-Blu-ray-Tom-Hanks/dp/B00275EHJQ/ref=lp_712256_1_1_sspa/130-7274635-2308352?s=movies-tv&ie=UTF8&qid=1552048730&sr=1-1-spons&psc=1&smid=A3V1KLU0LMW5KE
# B00004WGE7 2011593