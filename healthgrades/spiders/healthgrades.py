# -*- coding: utf-8 -*-#
from __future__ import absolute_import, division, unicode_literals

import json

import scrapy
import re
import requests
import xlrd
from urllib.parse import urljoin
from lxml import html

import os

base_dir_path = os.path.dirname(os.path.realpath(__file__))


class HealthgradesItem(scrapy.Item):
    Business_Title = scrapy.Field()
    Gender = scrapy.Field()
    Birthday = scrapy.Field()
    Age = scrapy.Field()
    Business_Categories = scrapy.Field()
    Office_Providers = scrapy.Field()
    Website = scrapy.Field()
    Procedures = scrapy.Field()
    Languages = scrapy.Field()
    Specialities = scrapy.Field()
    Education = scrapy.Field()
    Awards = scrapy.Field()
    Business_Contacts = scrapy.Field()
    Address = scrapy.Field()
    Reviews = scrapy.Field()
    Clinical_Quality_Ratings = scrapy.Field()
    Business_Hours = scrapy.Field()
    Memo = scrapy.Field()
    Insurances = scrapy.Field()
    pass


class HealthgradesSpider(scrapy.Spider):

    """
    Healthgrades data import
    """
    review_count = meta_count = 1
    name = "healthgrades_crawler"
    allowed_domains = ['www.healthgrades.com']
    start_urls = ['https://www.healthgrades.com/group-directory/fl-florida/kissimmee/usa-medical-care-y5d5wg']
    header = {
        'User-Agent': 'Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)'
    }

    def start_requests(self):
        """

        :return:
        """

        file = xlrd.open_workbook(base_dir_path + "/Research Offices.xlsx")
        sheet = file.sheet_by_index(0)
        for cell in range(1, sheet.nrows):
            external_id = int(sheet.row_values(cell)[0])
            url = str(sheet.row_values(cell)[10])
            if url:
                yield scrapy.Request(url=url, callback=self.parse_product, dont_filter=True, headers=self.header,
                                     meta={'ext_id': external_id})

    def parse_product(self, response):
        """

        :param response:
        :return:
        """
        # Meta data import calling
        self.get_meta_data(response)
        # Review data import calling
        self.get_reviews(response)

    def get_reviews(self, response):
        """

        :param response:
        :return:
        """
        item = {}
        rev_contnt = {}
        ext_sys_u_id = response.meta['ext_id']
        rev_list = []
        business_title = response.xpath('//h1[@itemprop="name"]/text()').extract()
        if not business_title:
            business_title = response.xpath('//div[@class="summary-hero-address"]/h1/text()').extract()
        business_title = business_title[0] if business_title else None
        total_reviews = response.xpath('//span[contains(@class, "review-count")]/text()').re('\d+')
        avg_rating = response.xpath('//div[@class="overall-rating"]/p/strong/text()').extract()
        if not avg_rating:
            avg_rating = response.xpath('//span[@class="reviewCount"]/text()').extract()
        stars = response.xpath('//tr[@class="breakdown-row"]/td[@class="count"]')
        rating = {
            'Total Reviews': total_reviews[0] if total_reviews else None,
            'Average Rating': avg_rating[0] if avg_rating else None,
            '5 stars': stars[0].re('\d+')[3] if stars else None,
            '4 stars': stars[1].re('\d+')[3] if stars else None,
            '3 stars': stars[2].re('\d+')[3] if stars else None,
            '2 stars': stars[3].re('\d+')[3] if stars else None,
            '1 stars': stars[4].re('\d+')[3] if stars else None,
        }

        reviews = []
        photos_list = []
        review_data = response.xpath('//div[@class="c-comment-list"]/div[@itemprop="review"]')

        for data in review_data:
            reviewer_name = data.xpath('.//span[@itemprop="author"]/text()').extract()
            reviewer_rating = data.xpath('.//div[@class="star-rating"]/div[@class="filled"]/span').extract()
            reviewer_date = data.xpath('.//span[@itemprop="datePublished"]/text()').extract()
            reviewer_content = data.xpath('.//div[@itemprop="reviewBody"]/text()').extract()

            if not reviewer_content:
                reviewer_div = data.xpath('.//div[@itemprop="reviewBody"]')
                reviewer_content = reviewer_div[0].xpath('.//span').extract()
                # print(reviewer_content[0],'\n')
                try:
                    reviewer_content =str(reviewer_content[0]).split('-->')[1]
                    reviewer_content = reviewer_content.split('<!--')[0]

                except IndexError:
                    print('Index Error')
            else:
                reviewer_content = reviewer_content[0]
            photos = {
                "description":"",
                "url":""
            }

            photos_list.append(photos)
            if 'photos' in rev_contnt:
                pass
            else:
                rev_contnt={
                    "content":reviewer_content if reviewer_content else None,
                    "photos":photos_list,
                    "provider":"healthgrades",
                    "review_title":"",
                    "reviewer_name":reviewer_name[0] if reviewer_name else None,
                    "stars":len(reviewer_rating)

                }

            rev_list.append(rev_contnt)

            reviews.append({
                'Name of Reviewer': reviewer_name[0] if reviewer_name else None,
                'Rating': len(reviewer_rating),
                'Review Date': reviewer_date[0] if reviewer_date else None,
                'Review Content': reviewer_content if reviewer_content else None
            })
        # item['Reviews'] = {
        #     'Rating': rating,
        #     'Review': reviews,
        #     'Business Title': business_title
        # }
        rev = {
            'Rating': rating,
            'Review': reviews,
            'Business Title': business_title
        }

        item['external_system_unique_id'] = ext_sys_u_id
        item['reviews'] = rev

        result = {
            'external_system_unique_id': ext_sys_u_id,
            'reviews': rev_list,
            "total_reviews": len(rev_list),
            "total_average_rating": avg_rating[0] if avg_rating else None,
        }

        # filename = base_dir_path + '/Review/output/' + 'reviews-' + str(self.review_count) + '.json'
        filename = base_dir_path + '/Review/output/' + 'reviews-' + str(response.meta['ext_id']) + '.json'
        with open(filename, 'w+') as output:
            json.dump(result, output, indent=4, sort_keys=True)
        self.review_count += 1
        return response

    def get_meta_data(self, response):
        """

        :param response:
        :return:
        """
        # item = HealthgradesItem()
        item = {}
        # item['external_system_unique_id'] = response.meta['ext_id']


        business_title = response.xpath('//h1[@itemprop="name"]/text()').extract()
        if not business_title:
            business_title = response.xpath('//div[@class="summary-hero-address"]/h1/text()').extract()
        # item['Business_Title'] = business_title[0] if business_title else None
        bus_title = business_title[0] if business_title else None

        gender = response.xpath('//div[@class="provider-gender"]/span[1]/text()').extract()
        # item['Gender'] = gender[0] if gender else None
        gen = gender[0] if gender else None

        age = response.xpath('//div[@class="provider-age"]/span/text()').re('\d+')
        # item['Age'] = age[0] if age else None
        p_age = age[0] if age else None

        business_categories = response.xpath('//div[@class="provider-speciality"]/span[1]/text()').extract()
        if not business_categories:
            business_categories = response.xpath('//p[@class="specialty"]/text()').extract()
        # item['Business_Categories'] = business_categories[0] if business_categories else None
        bus_cat = business_categories[0] if business_categories else None

        category_name = response.xpath('//li[@class="about-me-listitem"]/text()').extract()
        category_name = category_name[0] if category_name else None
        category = {
            'Name': category_name,
            'Description': None
        }
        # item['Specialities'] = {
        #     'Category': category,
        #     'Expertise Skills': None
        # }
        spec = {
            'Category': category,
            'Expertise Skills': None
        }

        school_name = response.xpath('//div[@class="education-completed"]/text()').extract()
        school_since = response.xpath('//div[@class="timeline-date-mobile"]/text()').extract()
        # item['Education'] = {
        #     'School Name': school_name[0] if school_name else None,
        #     'Department': None,
        #     'Since': school_since[0] if school_since else None,
        #     'By': None
        # }
        educ = {
            'School Name': school_name[0] if school_name else None,
            'Department': None,
            'Since': school_since[0] if school_since else None,
            'By': None
        }

        # office_phone = response.xpath('//a[@id="phone-summary-click"]/text()').extract()
        office_phone = response.xpath('//ul[@class="sr-only"]//a[@class="hg-track"]/@href').extract()
        if not office_phone:
            office_phone = response.xpath('//a[@class="tel"]/text()').extract()
        if not office_phone:
            office_phone = response.xpath('//div[@class="phone-number"]/a[@class="hg-track"]/text()').extract()
        # item['Business_Contacts'] = {
        #     'Office Phone': office_phone[0].replace('tel:', '') if office_phone else None,
        #     'Mobile Phone': None,
        #     'Email': None,
        #     'Fax': None
        # }
        bus_cntct = {
            'Office Phone': office_phone[0].replace('tel:', '') if office_phone else None,
            'Mobile Phone': None,
            'Email': None,
            'Fax': None
        }

        address = response.xpath('//p[@itemprop="streetAddress"]/text()').extract()
        if not address:
            address = response.xpath('//span[@itemprop="streetAddress"]/text()').extract()
        city = response.xpath('//span[@itemprop="addressLocality"]/text()').extract()
        state = response.xpath('//span[@itemprop="addressRegion"]/text()').extract()
        zip = response.xpath('//span[@itemprop="postalCode"]/text()').extract()
        # item['Address'] = {
        #     'Address': address[0] if address else None,
        #     'City': city[0] if city else None,
        #     'State': state[0] if state else None,
        #     'Zipcode': zip[0] if zip else None,
        #     'Lat/Lng': None,
        #     'Locatioon Alias/URL': None
        # }
        addr = {
            'Address': address[0] if address else None,
            'City': city[0] if city else None,
            'State': state[0] if state else None,
            'Zipcode': zip[0] if zip else None,
            'Lat/Lng': None,
            'Locatioon Alias/URL': None
        }
        memo = response.xpath('//span[@class="generated-bio"]/text()').extract()
        if not memo:
            memo = response.xpath('//div[@class="about-us learnAboutSection"]/ul/li/p/text()').extract()
        if not memo:
            memo = response.xpath('//div[contains(@class, "summary-awards")]/p[@class="graph-text"]/text()').extract()
        # item['Memo'] = memo[0] if memo else None
        mem = memo[0] if memo else None

        insurance_items = response.xpath('//ul[contains(@class, "insurance-list")]/li/text()').extract()
        # item['Insurances'] = {
        #     'Brand': None,
        #     'Insurance Items': insurance_items
        # }
        insure = {
            'Brand': None,
            'Insurance Items': insurance_items
        }

        # Office providere
        overview_links = response.xpath('//div[@class="provider-wrap__view-profile"]/a/@href').extract()
        overview_list = []
        for link in overview_links:
            try:
                data = requests.get(urljoin(response.url, link))
                data = html.fromstring(data.text)

                doctor_name = data.xpath('//h1[@itemprop="name"]/text()')
                specialization = data.xpath('//span[@class="generated-bio"]/text()')
                # phone = data.xpath('//a[@id="phone-summary-click"]/text()')
                # business_name = data.xpath('//div[@class="standard-address"]/h3/text()')
                # address = data.xpath('//p[@itemprop="streetAddress"]/text()')
                # city = data.xpath('//span[@itemprop="addressLocality"]/text()')
                # state = data.xpath('//span[@itemprop="addressRegion"]/text()')
                # zip = data.xpath('//span[@itemprop="postalCode"]/text()')
                average_rating = data.xpath('//div[@class="overall-rating"]/p/strong/text()')
                total_reviews = data.xpath('//span[@class="rating-labels"]/span/text()')
                total = None
                if total_reviews:
                    total = re.search('(\d+)', total_reviews[0], re.DOTALL)

                overview_list.append(
                    {
                        'Business Title': doctor_name[0],
                        'Business Categories': specialization[0],
                        'Profile Link': urljoin(response.url, link),
                        'Average Rating': average_rating[0] if average_rating else None,
                        'Total Reviews': total.group(1) if total else None,
                        # 'Phone number': phone[0],
                        # 'Business name': business_name[0],
                        # 'Address': address[0],
                        # 'City': city[0],
                        # 'State': state[0],
                        # 'Zip': zip[0]
                    }
                )

            except IndexError:
                print('Error while parsing overview content')

        # item['Office_Providers'] = overview_list
        office_prov = overview_list

        procedures = response.xpath('//ul[@class="services-list col3List"]/li/text()').extract()
        # item['Procedures'] = procedures
        proced = procedures

        languages = response.xpath('//div[contains(@class, "language-services")]/ul/li/text()').extract()
        # item['Languages'] = languages
        lang = languages

        business_hours = response.xpath('//div[@class="disclaimer-tooltip"]/ul/li')
        hour_list = []
        for hour in business_hours:
            hours = hour.xpath('./text()').extract()
            hour_list.append(' '.join(hours))
        # item['Business_Hours'] = {
        #     'Monday': hour_list[0] if hour_list else None,
        #     'Tuesday': hour_list[1] if hour_list else None,
        #     'Wednesday': hour_list[2] if hour_list else None,
        #     'Thursday': hour_list[3] if hour_list else None,
        #     'Friday': hour_list[4] if hour_list else None,
        #     'Saturday': hour_list[5] if hour_list else None,
        #     'Sunday': hour_list[6] if hour_list else None
        # }
        bus_hr = {
            'Monday': hour_list[0] if hour_list else None,
            'Tuesday': hour_list[1] if hour_list else None,
            'Wednesday': hour_list[2] if hour_list else None,
            'Thursday': hour_list[3] if hour_list else None,
            'Friday': hour_list[4] if hour_list else None,
            'Saturday': hour_list[5] if hour_list else None,
            'Sunday': hour_list[6] if hour_list else None
        }

        awards = response.xpath('//div[@class="awards-text"]/h2/text()').extract()
        awards_list = response.xpath('//ul[@class="hg3-awards"]/li/meta/@content').extract()
        # item['Awards'] = {
        #     'Category': awards[0] if awards else None,
        #     'Awards list by cate award': awards_list
        # }
        award = {
            'Category': awards[0] if awards else None,
            'Awards list by cate award': awards_list
        }

        overall_ratio = response.xpath(
            '//div[@class="columns medium main-graph-radial-data"]/@data-outer-percent').extract()
        clincial_list = []
        clincials = response.xpath('//div[@class="clinical-quality-overlay hg3-overlay"]')
        for cli in clincials:
            cli_category = cli.xpath('.//div[@class="overlay-header"]/h2/text()').extract()
            cli_category = cli_category[0].replace('Clinical Quality: ', '') if cli_category else None
            mortality_list = []
            # mortalities = response.xpath('//div[@class="hg3-clinical-quality-grid"]'
            #                              '/h4[contains(text(), "Mortality Based Ratings")]')
            mortalities = cli.xpath(
                './/div[@class="clinical-overlay-grid-container"]/div[@class="hg3-clinical-quality-grid"][1]'
                '/div[contains(@class, "clpseable")]')
            for mor in mortalities:
                mortality_list.append({
                    'Mortality': mor.xpath('.//h6[@class="title"]/text()').extract_first(),
                    'Description': ''.join(mor.xpath('.//p[@class="based-rating"]//text()').extract()),
                    'Actual Mortality': {
                        'Mortality1': mor.xpath(
                            './/div[@class="row inner js-clpse hidden actual-num"]/div[@class="tb-col number"]'
                            '/text()').extract()[0],
                        'Mortality2': mor.xpath(
                            './/div[@class="row inner js-clpse hidden actual-num"]/div[@class="tb-col number"]'
                            '/text()').extract()[1]
                    },
                    'Predicted Mortality': {
                        'Mortality1': mor.xpath(
                            './/div[@class="row inner js-clpse hidden predicted-num"]/div[@class="tb-col number"]'
                            '/text()').extract()[0],
                        'Mortality2': mor.xpath(
                            './/div[@class="row inner js-clpse hidden predicted-num"]/div[@class="tb-col number"]'
                            '/text()').extract()[1]
                    },
                    'Question pool': {
                        'Question': mor.xpath(
                            './/div[@class="explanation"]/h3[@class="headline"]/text()').extract_first(),
                        'Answer': mor.xpath(
                            './/div[@class="explanation"]/div[@class="summary"]/p[1]/text()').extract_first()
                    }
                })

            complication_list = []

            # complications = response.xpath('//div[@class="hg3-clinical-quality-grid"]'
            #                                '/h4[contains(text(), "Complication Based Ratings")]')
            complications = cli.xpath('.//div[@class="clinical-overlay-grid-container"]/div[2]'
                                      '/div[contains(@class, "clpseable")]')
            for com in complications:
                complication_list.append({
                    'Complication': com.xpath('.//h6[@class="title"]/text()').extract_first(),
                    'Description': ''.join(mor.xpath('.//p[@class="based-rating"]//text()').extract()),
                    'Actual': {
                        'Hospital': com.xpath(
                            './/div[@class="row inner js-clpse hidden actual-num"]/div[@class="tb-col number"]'
                            '/text()').extract()[0],
                        'National': com.xpath(
                            './/div[@class="row inner js-clpse hidden actual-num"]/div[@class="tb-col number"]'
                            '/text()').extract()[1]
                    },
                    'Predicted': {
                        'Hospital': com.xpath(
                            './/div[@class="row inner js-clpse hidden predicted-num"]/div[@class="tb-col number"]'
                            '/text()').extract()[0],
                        'National': com.xpath(
                            './/div[@class="row inner js-clpse hidden predicted-num"]/div[@class="tb-col number"]'
                            '/text()').extract()[1]
                    },
                    'Question pool': {
                        'Question': com.xpath(
                            './/div[@class="explanation"]/h3[@class="headline"]/text()').extract_first(),
                        'Answer': com.xpath(
                            './/div[@class="explanation"]/div[@class="summary"]/p[1]/text()').extract_first()
                    }
                })

            clincial_list.append({
                'Category': cli_category,
                'Mortality Based Ratings': mortality_list,
                'Complication Based Ratings': complication_list
            })
        # item['Clinical_Quality_Ratings'] = {
        #     'Overall Recommended Ratio': overall_ratio[0] if overall_ratio else None,
        #     'Clinical Quality Ratings': clincial_list
        # }
        clinic_q_rate = {
            'Overall Recommended Ratio': overall_ratio[0] if overall_ratio else None,
            'Clinical Quality Ratings': clincial_list
        }

        item['Business_Title']=bus_title
        item['Gender']=gen
        item['Age']=p_age
        item['Business_Categories']=bus_cat
        item['Specialities']=spec
        item['Education']=educ
        item['Business_Contacts']=bus_cntct
        item['Address']=addr
        item['Memo']=mem
        item['Insurances']=insure
        item['Office_Providers']=office_prov
        item['Procedures']=proced
        item['Languages']=lang
        item['Business_Hours']=bus_hr
        item['Awards']=award
        item['Clinical_Quality_Ratings']=clinic_q_rate


    # filename = base_dir_path + '/output/' + 'business-' + str(self.meta_count) + '.json'
        filename = base_dir_path + '/output/' + 'business-' + str(response.meta['ext_id']) + '.json'
        with open(filename, 'w+') as output:
            json.dump(item, output, indent=4, sort_keys=True)
        self.meta_count += 1
        return response
