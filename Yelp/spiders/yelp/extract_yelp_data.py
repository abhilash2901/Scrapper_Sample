#!/usr/bin/python

import re
import json
import datetime
import requests

from lxml import html
from spiders.extract_data import Scraper


class YelpScraper(Scraper):
    ##########################################
    ############### PREP
    ##########################################

    INVALID_URL_MESSAGE = "Expected URL format is ^https://(www|en).yelp.com/biz/([a-zA-Z0-9-]+-)?[a-zA-Z0-9]+$"

    def __init__(self, **kwargs):  # **kwargs are presumably (url, bot)
        Scraper.__init__(self, **kwargs)

        self.page_json = None
        self.business_hours = None

    def rebuild_business_url(self):
        return self.business_page_url

    def check_url_format(self):
        """Checks business URL format for this scraper instance is valid.
        Returns:
            True if valid, False otherwise
        """

        # http://www.yelp.com/businesss/calvin-klein-fragrances-ck-one-summer-P590003965793470000.jsp
        m = re.match(r"^https://(www|en).yelp.com(.ph)?/biz/([a-zA-Z0-9-]+-)?[a-zA-Z0-9]+(\?.*)?$",
                     self.business_page_url)
        return not not m

    def not_a_business(self):
        """Checks if current page is not a valid business page
        (an unavailable business page or other type of method)
        Overwrites dummy base class method.
        Returns:
            True if it's an unavailable business page
            False otherwise
        """
        try:
            itemtype = self.tree_html.xpath('//meta[@property="og:type"]/@content')[0].strip()

            if itemtype != "yelpyelp:business":
                raise Exception()

        except Exception:
            return True

        self._extract_page_json()

        return False

    ##########################################
    ############### HELPER FUNCTIONS
    ##########################################

    def _remove_comments(self, string):
        string = re.sub(re.compile("/\*.*?\*/", re.DOTALL), "",
                        string)  # remove all occurance streamed comments (/*COMMENT */) from string
        return string

    def _extract_page_json(self):
        if self.page_json:
            return

        page_json = {}
        categories = []

        try:
            page_jsons_list = self.tree_html.xpath("//script[@type='application/ld+json']/text()")

            for raw_json in page_jsons_list:
                json_info = json.loads(raw_json)

                if json_info.get("@type") == "LocalBusiness":
                    page_json = json_info
                elif "itemListElement" in json_info:
                    categories.append(json_info["itemListElement"][-1]["item"]["name"])

            working_hours_by_day = self.tree_html.xpath("//table[@class='table table-simple hours-table']/tbody/tr")
            business_hours = {'Mon': None, 'Tue': None, 'Wed': None, 'Thu': None, 'Fri': None, 'Sat': None, 'Sun': None}

            for working_hours in working_hours_by_day:
                business_hours[working_hours.xpath('./th/text()')[0].strip()] = working_hours.xpath('./td')[
                    0].text_content().strip().split(" - ")

            self.business_hours = business_hours

        except Exception as e:
            print("Parsing error in page json :" + str(e))

        if page_json:
            self.page_json = page_json

            if categories:
                self.page_json["businessCategories"] = categories

    ##########################################
    ############### FIELD FUNCTIONS
    ##########################################
    def _business_name(self):
        return self.page_json.get("name")

    def _business_description(self):
        return None

    def _price(self):
        return None

    def _before_price_label(self):
        return None

    def _after_price_label(self):
        return None

    def _phone(self):
        business_phone = self.tree_html.xpath("//span[@class='biz-phone']/text()")
        return business_phone[0].strip() if business_phone else None,

    def _website(self):
        website = self.tree_html.xpath("//span[contains(@class, 'biz-website')]//a/text()")

        return website[0].strip() if website else None

    def _google_place_id(self):
        return None

    def _yelp_id(self):
        yelp_biz_id = self.tree_html.xpath("//meta[@name='yelp-biz-id']/@content")

        return yelp_biz_id[0].strip() if yelp_biz_id else None

    def _lat(self):
        return None

    def _lng(self):
        return None

    def _address(self):
        return self.page_json.get("address", {}).get("streetAddress")

    def _city(self):
        return self.page_json.get("address", {}).get("addressLocality")

    def _state(self):
        return self.page_json.get("address", {}).get("addressRegion")

    def _zip(self):
        return self.page_json.get("address", {}).get("postalCode")

    def _monday_open_time(self):
        if self.business_hours and self.business_hours.get("Mon"):
            return self.business_hours["Mon"][0]

        return None

    def _monday_close_time(self):
        if self.business_hours and self.business_hours.get("Mon"):
            return self.business_hours["Mon"][1] if len(self.business_hours["Mon"]) > 1 else self.business_hours["Mon"][
                0]

        return None

    def _tuesday_open_time(self):
        if self.business_hours and self.business_hours.get("Tue"):
            return self.business_hours["Tue"][0]

        return None

    def _tuesday_close_time(self):
        if self.business_hours and self.business_hours.get("Tue"):
            return self.business_hours["Tue"][1] if len(self.business_hours["Tue"]) > 1 else self.business_hours["Tue"][
                0]

        return None

    def _wednesday_open_time(self):
        if self.business_hours and self.business_hours.get("Wed"):
            return self.business_hours["Wed"][0]

        return None

    def _wednesday_close_time(self):
        if self.business_hours and self.business_hours.get("Wed"):
            return self.business_hours["Wed"][1] if len(self.business_hours["Wed"]) > 1 else self.business_hours["Wed"][
                0]

        return None

    def _thursday_open_time(self):
        if self.business_hours and self.business_hours.get("Thu"):
            return self.business_hours["Thu"][0]

        return None

    def _thursday_close_time(self):
        if self.business_hours and self.business_hours.get("Thu"):
            return self.business_hours["Thu"][1] if len(self.business_hours["Thu"]) > 1 else self.business_hours["Thu"][
                0]

        return None

    def _friday_open_time(self):
        if self.business_hours and self.business_hours.get("Fri"):
            return self.business_hours["Fri"][0]

        return None

    def _friday_close_time(self):
        if self.business_hours and self.business_hours.get("Fri"):
            return self.business_hours["Fri"][1] if len(self.business_hours["Fri"]) > 1 else self.business_hours["Fri"][
                0]

        return None

    def _saturday_open_time(self):
        if self.business_hours and self.business_hours.get("Sat"):
            return self.business_hours["Sat"][0]

        return None

    def _saturday_close_time(self):
        if self.business_hours and self.business_hours.get("Sat"):
            return self.business_hours["Sat"][1] if len(self.business_hours["Sat"]) > 1 else self.business_hours["Sat"][
                0]

        return None

    def _sunday_open_time(self):
        if self.business_hours and self.business_hours.get("Sun"):
            return self.business_hours["Sun"][0]

        return None

    def _sunday_close_time(self):
        if self.business_hours and self.business_hours.get("Sun"):
            return self.business_hours["Sun"][1] if len(self.business_hours["Sun"]) > 1 else self.business_hours["Sun"][
                0]

        return None

    def _categories(self):
        return self.page_json.get("businessCategories")

    # return image url and description and None if it is empty
    def _photos(self):
        photos_list = []

        for showcase_photo in self.tree_html.xpath("//div[@class='showcase-photos']/div"):
            if showcase_photo.xpath(".//div[@class='photo-box-overlay_caption']/text()"):
                url = showcase_photo.xpath(".//img/@src")[0],
                description = showcase_photo.xpath(".//div[@class='photo-box-overlay_caption']/text()")[0]

                if '/bphoto/' in str(url):
                    print("ISIDE IFFFFFFFF       ",url)
                    url_link = 'null'
                    try:
                        for link in url:

                            url_link = link
                    except:
                        pass
                    photo_info = {'description': description,
                                    'url': url_link
                                  }

                    photos_list.append(photo_info)
                else:
                    pass
        if photos_list:
            return photos_list
        else:
            return None
    #return review details
    def _reviews(self):
        reviews = {'rating': {'totalReviews': self.page_json.get("aggregateRating", {}).get("reviewCount", 0),
                              'averageRating': self.page_json.get("aggregateRating", {}).get("ratingValue", 0),
                              '5 stars': 0,
                              '4 stars': 0,
                              '3 stars': 0,
                              '2 stars': 0,
                              '1 stars': 0},
                   'review': []}

        for review in self.page_json.get("review", []):
            reviews['rating']['{} stars'.format(review["reviewRating"]["ratingValue"])] += 1
            detail_review = {'nameOfReviewer': review["author"],
                             'rating': review["reviewRating"]["ratingValue"],
                             'reviewDate': review["datePublished"],
                             'reviewContent': review["description"]}
            reviews['review'].append(detail_review)

        return reviews

    ##########################################
    ################ RETURN TYPES
    ##########################################

    # dictionaries mapping type of info to be extracted to the method that does it
    # also used to define types of data that can be requested to the REST service
    # print("____---------------                  ",_photos)
    DATA_TYPES = {
        "business_name": _business_name,
        "business_description": _business_description,
        "price": _price,
        "before_price_label": _before_price_label,
        "after_price_label": _after_price_label,
        "phone": _phone,
        "website": _website,
        "google_place_id": _google_place_id,
        "yelp_id": _yelp_id,
        "lat": _lat,
        "lng": _lng,
        "address": _address,
        "city": _city,
        "state": _state,
        "zip": _zip,
        "monday_open_time": _monday_open_time,
        "monday_close_time": _monday_close_time,
        "tuesday_open_time": _tuesday_open_time,
        "tuesday_close_time": _tuesday_close_time,
        "wednesday_open_time": _wednesday_open_time,
        "wednesday_close_time": _wednesday_close_time,
        "thursday_open_time": _thursday_open_time,
        "thursday_close_time": _thursday_close_time,
        "friday_open_time": _friday_open_time,
        "friday_close_time": _friday_close_time,
        "saturday_open_time": _saturday_open_time,
        "saturday_close_time": _saturday_close_time,
        "sunday_open_time": _sunday_open_time,
        "sunday_close_time": _sunday_close_time,
        "categories": _categories,
        "photos": _photos,
        #        "reviews": _reviews
    }
