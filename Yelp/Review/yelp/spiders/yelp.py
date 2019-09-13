# -*- coding: utf-8 -*-#
from __future__ import absolute_import, division, unicode_literals

import scrapy
import xlrd
from datetime import datetime
import time
import json
from scrapy.conf import settings

import os

base_dir_path = os.path.dirname(os.path.realpath(__file__))


def recheck_rev_json(data):
    rev_key = data.get('reviews')
    try:
        flag = 0
        for key in rev_key:
            # print("kkkk     ",key)
            url = key['photos']['url']
            # print("\n\nURLLlll       \n",url,'\n\n')
            if 'bphoto' in str(url) or '/biz_photos/' in str(url):
                flag = 0
                # print("INSIDE       IFFFF           ",url)
            else:
                pass
        # if flag == 1:
        #         key['photos'] = 'null'
    except:
        pass

    return data


class YelpSpider(scrapy.Spider):
    name = "yelp_crawler"
    allowed_domains = ['www.yelp.com']
    start_urls = ['https://www.yelp.com/biz/premier-medical-associates-bushnell?osq=Premier+Medical+Associates']
    header = {
        'User-Agent': 'Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)'
    }

    settings.overrides['ROBOTSTXT_OBEY'] = False

    def start_requests(self):
        url_list = []
        file = xlrd.open_workbook(base_dir_path + "/Research Offices.xlsx")
        sheet = file.sheet_by_index(0)
        for k in range(1, sheet.nrows):
            external_id = int(sheet.row_values(k)[0])
            url_list.append({'id': external_id, 'url': str(sheet.row_values(k)[9])})
        for url in url_list:
            if url['url'] and 'yelp.com' in url['url']:
                yield scrapy.Request(url=url['url'], callback=self.parse_product,
                                     dont_filter=True, headers=self.header,
                                     meta={'external_id': url['id'],'urls':url['url']})

    def parse_product(self, response):

        """

        :param response:
        :return:
        """
        total_rating = 0
        review_list = []
        external_id = response.meta['external_id']
        review_contents_list = response.xpath('//div[@class="review-list"]/ul'
                                              '/li')[1:]

        url_lists = []
        # for showcase_photo in response.xpath("//div[@class='showcase-photos']/div"):
        for showcase_photo in response.xpath(".//ul[@class='photo-box-grid']"):
            # print("dfgdfgdfgdfg",showcase_photo.xpath("//li").xpath(".//div[@class='photo-box']"))
            try:

                for photo_bx in showcase_photo.xpath(".//div[@class='photo-box']/div"):
                    print(photo_bx,"photo_bxphoto_bxphoto_bxphoto_bx")
                    desc = ""
                    url = photo_bx.xpath(".//img/@src").extract()[0]
                    if '/bphoto/' in str(url) or '/biz_photos/' in str(url):

                        if  photo_bx.xpath(".//span[@class='offscreen']/text()").extract():
                            desc =  photo_bx.xpath(".//div[@class='photo-box-overlay_caption']/text()").extract()[0]

                        url_lists.append({'description':desc,'url':url})
                    else:
                        pass
            except:
                pass


        photos = url_lists


        for review_contents in review_contents_list:
            reviewer_name = review_contents.xpath('.//div[contains(@class, "review--with-sidebar")]'
                                                  '//li[@class="user-name"]/a/text()')[0].extract()

            # url = review_contents.xpath('.//div[@class="review-sidebar"]'
            #                                  '//img[@class="photo-box-img"]/@src')[0].extract()

            # url = showcase_photo.xpath(".//img/@src")[0],
            # description = showcase_photo.xpath(".//div[@class='photo-box-overlay_caption']/text()")[0]
            # sets url null if default image url is present
            # if '/default_avatars/' in str(url):
            #     url = 'null'
            #
            # photos = {
            #     'description': '',
            #     'url':url
            #
            # }

            stars = int(float(review_contents.xpath('.//div[@class="review-content"]'
                                                    '//div[contains(@class, "i-stars")]'
                                                    '/@title').re('(\d+\.\d+)')[0]))
            timestamp = review_contents.xpath('.//div[@class="review-content"]'
                                              '//span[@class="rating-qualifier"]/text()')[0].extract().strip()
            timestamp = datetime.strptime(timestamp, '%m/%d/%Y')
            timestamp = int(time.mktime(timestamp.timetuple()) + timestamp.microsecond / 1000000.0)

            content = review_contents.xpath('.//div[@class="review-content"]/p/text()').extract()
            content = '. '.join(content)

            reviews = {
                'provider': 'yelp',
                'reviewer_name': reviewer_name,
                'review_title': '',
                'content': content,
                'timestamp': timestamp,
                'stars': stars,
                'photos': photos,
            }

            review_list.append(reviews)
            total_rating += stars

        try:
            total_average_rating = total_rating / len(review_list)
        except ZeroDivisionError:
            total_average_rating = 0

        result = {
            'external_system_unique_id': external_id,
            'reviews': review_list,
            "total_reviews": len(review_list),
            "total_average_rating": total_average_rating,
        }

        filename = base_dir_path + '/output/' + 'reviews-' + str(external_id) + '.json'
        with open(filename, 'w+') as output:
            result = recheck_rev_json(result)
            json.dump(result, output, indent=4, sort_keys=True)
        yield
