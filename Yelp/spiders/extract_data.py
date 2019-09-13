# -*- coding: utf-8 -*-

 #!/usr/bin/python

import urllib.request
import requests
from requests.auth import HTTPProxyAuth
import re
import sys
import time
import random
from urllib.request import urlopen
from socket import timeout
from http.client import IncompleteRead
from lxml import html, etree
from itertools import chain
from html.parser import HTMLParser

from random import randint


class Scraper():

    """Base class for scrapers
    Handles incoming requests and calls specific methods from subclasses
    for each requested type of data,
    making sure to minimize number of requests to the site being scraped

    Each subclass must implement:
    - define DATA_TYPES and DATA_TYPES_SPECIAL structures (see subclass docs)
    - implement each method found in the values of the structures above
    - implement checktree_html_format()

    Attributes:
        business_page_url (string): URL of the page of the business being scraped
        tree_html (lxml tree object): html tree of page source. This variable is initialized
        whenever a request is made for a piece of data in DATA_TYPES. So it can be used for methods
        extracting these types of data.
        MAX_RETRIES (int): number of retries before giving up fetching business page soruce (if errors encountered
            - usually IncompleteRead exceptions)
    """
    BROWSER_AGENT_STRING_LIST = {"Firefox": ["Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
                                             "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
                                             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0"],
                                 "Chrome":  ["Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
                                             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
                                             "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
                                             "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36"],
                                 "Safari":  ["Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A",
                                             "Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25",
                                             "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13+ (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2"]
                                 }

    def select_browser_agents_randomly(self, agent_type=None):
        if agent_type and agent_type in self.BROWSER_AGENT_STRING_LIST:
            return random.choice(self.BROWSER_AGENT_STRING_LIST[agent_type])

        return random.choice(random.choice(self.BROWSER_AGENT_STRING_LIST.values()))

    PLATFORM_AGENT_STRING_LIST = ["desktop"]

    def select_platform_agents_randomly(self):
        return random.choice(self.PLATFORM_AGENT_STRING_LIST)

    # number of retries for fetching business page source before giving up
    MAX_RETRIES = 100

    # List containing all data types returned by the crawler (that will appear in responses of requests to service in crawler_service.py)
    # In practice, all returned data types for all crawlers should be defined here
    # The final list containing actual implementing methods for each data type will be defined in the constructor
    # using the declarations in the subclasses (for data types that have support in each subclass)

    BASE_DATA_TYPES_LIST = {
        "business_name",
        "business_description",
        "price",
        "before_price_label",
        "after_price_label",
        "phone",
        "website",
        "google_place_id",
        "yelp_id",
        "lat",
        "lng",
        "address",
        "city",
        "state",
        "zip",
        "monday_open_time",
        "monday_close_time",
        "tuesday_open_time",
        "tuesday_close_time",
        "wednesday_open_time",
        "wednesday_close_time",
        "thursday_open_time",
        "thursday_close_time",
        "friday_open_time",
        "friday_close_time",
        "saturday_open_time",
        "saturday_close_time",
        "sunday_open_time",
        "sunday_close_time",
        "categories",
        "photos",
#        "reviews"
    }

    # "loaded_in_seconds" needs to always have a value of None (no need to implement extraction)
    # TODO: date should be implemented here
    BASE_DATA_TYPES = {
        data_type : lambda x: None for data_type in BASE_DATA_TYPES_LIST # using argument for lambda because it will be used with "self"
    }

    # response in case of error
    ERROR_RESPONSE = {
        "url": None,
        "event": None,
        "business_id": None,
        "date": None,
        "status": None,
        "failure_type": None,
        "owned": None
    }

    TAG_RE = re.compile(r'<[^>]+>')

    def remove_tags(self, text):
        return self.TAG_RE.sub('', text)

    def remove_duplication_keeping_order_in_list(self, seq):
        if seq:
            seen = set()
            seen_add = seen.add
            return [x for x in seq if not (x in seen or seen_add(x))]

        return None

    def load_page_from_url_with_number_of_retries(self,
                                                  url,
                                                  max_retries=MAX_RETRIES,
                                                  extra_exclude_condition=None,
                                                  stream=False):
        for index in range(1, max_retries):
            s = requests.Session()
            a = requests.adapters.HTTPAdapter(max_retries=self.MAX_RETRIES)
            b = requests.adapters.HTTPAdapter(max_retries=self.MAX_RETRIES)
            s.mount('http://', a)
            s.mount('https://', b)
            print("retries url : %s" % url)
            if self.proxy_config:
                header = {"X-Crawlera-UA": self.select_platform_agents_randomly()}
                r = s.get(url, headers=header, proxies=self.proxies, auth=self.proxy_auth, stream=stream)
            else:
                header = {"User-Agent": self.select_browser_agents_randomly()}
                r = s.get(url, headers=header, stream=stream)
            print("retries request status : %s" % r.status_code)
            if not stream:
                contents = r.text
                if (not extra_exclude_condition
                    or extra_exclude_condition not in contents) \
                        and r.status_code == 200:
                    return contents
            else:
                if r.status_code == 200:
                    return r
            print('xml crawler retry times: %s' % index)
            time.sleep(randint(6,12))

        return None

    def _exclude_javascript_from_description(self, description):
        description = re.subn(r'<(script).*?</\1>(?s)', '', description)[0]
        description = re.subn(r'<(style).*?</\1>(?s)', '', description)[0]
        description = re.subn("(<!--.*?-->)", "", description)[0]
        return description
    #clean the text by replacing \n &nbsp \t etc.
    def _clean_text(self, text):
        text = text.replace("\n", " ").replace("\t", " ").replace("\r", " ")
        text = re.sub("&nbsp;", " ", text).strip()

        return re.sub(r'\s+', ' ', text)

    def __init__(self, **kwargs):
        self.business_page_url = kwargs['url']
        self.bot_type = kwargs['bot']
        self.is_timeout = False
        # Set generic fields
        # directly (don't need to be computed by the scrapers)

        # Note: This needs to be done before merging with DATA_TYPES, below,
        # so that BASE_DATA_TYPES values can be overwritten by DATA_TYPES values
        # if needed. (more specifically overwrite functions for extracting certain data
        # (especially sellers-related fields))
        self.proxy_config = None
        if kwargs.get('proxies'):
            self.proxy_config = kwargs['proxies']
            proxy_host = self.proxy_config["host"]
            proxy_port = self.proxy_config["port"]
            self.proxy_auth = HTTPProxyAuth(self.proxy_config["apikey"], "")
            self.proxies = {"http": "http://{}:{}/".format(proxy_host, proxy_port)}

        self.headers = {
            'user-agent': 'Mozilla/5.0 (Linux; Android 5.1.1; YQ601 Build/LMY47V) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/39.0.0.0 Mobile Safari/537.36'
        }
        # update data types dictionary to overwrite names of implementing methods for each data type
        # with implmenting function from subclass
        # precaution mesaure in case one of the dicts is not defined in a scraper
        if not hasattr(self, "DATA_TYPES"):
            self.DATA_TYPES = {}
        if not hasattr(self, "DATA_TYPES_SPECIAL"):
            self.DATA_TYPES_SPECIAL = {}
        self.ALL_DATA_TYPES = self.BASE_DATA_TYPES.copy()
        self.ALL_DATA_TYPES.update(self.DATA_TYPES)
        self.ALL_DATA_TYPES.update(self.DATA_TYPES_SPECIAL)
        # remove data types that were not declared in this superclass

        # TODO: do this more efficiently?
        # for key in list(self.ALL_DATA_TYPES.keys()):
        #     if key not in self.BASE_DATA_TYPES:
        #         print("*******EXTRA data type: ", key)
        #         del self.ALL_DATA_TYPES[key]


    # extract business info from business page.
    # (note: this is for info that can be extracted directly from the business page, not content generated through javascript)
    # Additionally from extract_business_data(), this method extracts page load time.
    # parameter: types of info to be extracted as a list of strings, or None for all info
    # return: dictionary with type of info as key and extracted info as value
    def business_info(self, info_type_list = None):
        """Extract all requested data for this business, using subclass extractor methods
        Args:
            info_type_list (list of strings) list containing the types of data requested
        Returns:
            dictionary containing the requested data types as keys
            and the scraped data as values
        """

        #TODO: does this make sure page source is not extracted if not necessary?
        #      if so, should all functions returning null (in every case) be in DATA_TYPES_SPECIAL?

        # if no specific data types were requested, assume all data types were requested
        if not info_type_list:
            info_type_list = self.ALL_DATA_TYPES.keys()


        # copy of info list to send to _extract_business_data
        info_type_list_copy = list(info_type_list)

        # build page xml tree. also measure time it took and assume it's page load time (the rest is neglijable)
        time_start = time.time()
        #TODO: only do this if something in DATA_TYPES was requested
        if self.bot_type == "mobile":
            self._get_json_from_api()
        else:
            self._extract_page_tree()
        time_end = time.time()
        # don't pass load time as info to be extracted by _extract_business_data
        return_load_time = "loaded_in_seconds" in info_type_list_copy
        if return_load_time:
            info_type_list_copy.remove("loaded_in_seconds")

        ret_dict = self._extract_business_data(info_type_list_copy)
        # add load time to dictionary -- if it's in the list
        # TODO:
        #      - format for loaded_in_seconds?
        #      - what happens if there are requests to js info too? count that load time as well?
        if return_load_time:
            ret_dict["loaded_in_seconds"] = round(time_end - time_start, 2)

        return ret_dict

    # method that returns json from api
    def _get_json_from_api(self):
        for i in range(self.MAX_RETRIES):
            try:
                r = requests.get(self.business_page_url, headers=self.headers)
                if r.status_code == 200:
                    self.business_json = r.json()
                else:
                    continue
                #sleep(randint(0, 1))
                return
            except Exception as e:
                print(e)
                continue

    def _get_json_with_custom_api(self, url=None):
        for i in range(self.MAX_RETRIES):
            try:
                r = requests.get(url, headers=self.headers)
                if r.status_code == 200:
                    return r.json()
                else:
                    continue
                #sleep(randint(0, 1))
            except Exception as e:
                print(e)
                continue

    # method that returns xml tree of page, to extract the desired elemets from
    def _extract_page_tree(self, elemUrl=None):
        """Builds and sets as instance variable the xml tree of the business page
        Returns:
            lxml tree object
        """
        if elemUrl:
            request_url = elemUrl
        else:
            request_url = self.business_page_url

        request = urllib.request.Request(request_url)
        # set user agent to avoid blocking
        agent = ''
        if self.bot_type == "google":
            agent = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
        else:
            agent = 'Mozilla/5.0 (X11; Linux x86_64; rv:24.0) Gecko/20140319 Firefox/24.0 Iceweasel/24.4.0'

        request.add_header('User-Agent', agent)

        for i in range(self.MAX_RETRIES):
            # will using proxy request if first 3 retries failed
            if i > 3 and self.proxy_config:
                header = {"X-Crawlera-UA": self.select_platform_agents_randomly()}
                try:
                    r = requests.get(
                        request_url,
                        headers=header,
                        proxies=self.proxies,
                        auth=self.proxy_auth,
                        timeout=20)
                    if r.status_code == 200:
                        contents = r.text.encode("utf-8")
                        contents = self._clean_null(contents)
                        self.page_raw_text = contents
                        self.tree_html = html.fromstring(contents)
                        return
                    else:
                        raise Exception
                except Exception:
                    print('business crawler retry times: %s' % i)
                    time.sleep(randint(5, 7))
                    continue

            else:
                try:
                    contents = urllib.request.urlopen(request, timeout=20).read()
                # handle urls with special characters
                except UnicodeEncodeError as e:
                    request = urllib.request.Request(request_url.encode("utf-8"))
                    request.add_header('User-Agent', agent)
                    contents = urllib.request.urlopen(request).read()

                except IncompleteRead:
                    continue
                except timeout:
                    self.is_timeout = True
                    self.ERROR_RESPONSE["failure_type"] = "Timeout"
                    return
                except urllib.request.HTTPError as err:
                    if err.code == 404:
                        self.ERROR_RESPONSE["failure_type"] = "HTTP 404 - Page Not Found"
                        return
                    else:
                        raise
                try:
                    # replace NULL characters
                    contents = self._clean_null(contents.decode("utf8"))
                    self.page_raw_text = contents
                    self.tree_html = html.fromstring(contents)
                except UnicodeError as e:
                    # if string was not utf8, don't deocde it
                    print("Warning creating html tree from page content: ", e.message)

                    # replace NULL characters
                    contents = self._clean_null(contents)
                    self.page_raw_text = contents
                    self.tree_html = html.fromstring(contents)
                # if we got it we can exit the loop and stop retrying
                return

                # try getting it again, without catching exception.
                # if it had worked by now, it would have returned.
                # if it still doesn't work, it will throw exception.
                # TODO: catch in crawler_service so it returns an "Error communicating with server" as well

                # contents = urlopen(request).read()
                # replace NULL characters
                # contents = self._clean_null(contents)
                # self.page_raw_text = contents
                # self.tree_html = html.fromstring(contents)



    def _clean_null(self, text):
        '''Remove NULL characters from text if any.
        Return text without the NULL characters
        '''
        if text.find('\00') >= 0:
            print("WARNING: page contained NULL characters. Removed")
            text = text.replace('\00','')
        return text

    def _find_between(self, s, first, last, offset=0):
        try:
            start = s.index(first, offset) + len(first)
            end = s.index(last, start)
            return s[start:end]
        except ValueError:
            return ""

    # Extract business info given a list of the type of info needed.
    # Return dictionary containing type of info as keys and extracted info as values.
    # This method is intended to act as a unitary way of getting all data needed,
    # looking to avoid generating the html tree for each kind of data (if there is more than 1 requested).
    def _extract_business_data(self, info_type_list):
        """Extracts data for current business:
        either from page source given its xml tree
        or using other requests defined in each specific function
        Args:
            info_type_list: list of strings containing the requested data
        Returns:
            dictionary containing the requested data types as keys
            and the scraped data as values
        """

        results_dict = {}

        # if it's not a valid business page, abort
        if self.is_timeout or self.not_a_business():
            return self.ERROR_RESPONSE

        for info in info_type_list:
            try:
                if isinstance(self.ALL_DATA_TYPES[info], (str, str)):
                    _method_to_call = getattr(self, self.ALL_DATA_TYPES[info])
                    results = _method_to_call()
                else:  # callable, dict
                    _method_to_call = self.ALL_DATA_TYPES[info]
                    results = _method_to_call(self)
            except IndexError as e:
                sys.stderr.write("ERROR: No " + info + " for " + self.business_page_url.encode("utf-8") + ":\n" + str(e) + "\n")
                results = None
            except Exception as e:
                sys.stderr.write("ERROR: Unknown error extracting " + info + " for " + self.business_page_url.encode("utf-8") + ":\n" + str(e) + "\n")
                results = None

            results_dict[info] = results

        return results_dict

    # base function to test input URL is valid.
    # always returns True, to be used for subclasses where it is not implemented
    # it should be implemented by subclasses with specific code to validate the URL for the specific site
    def check_url_format(self):
        return True

    def not_a_business(self):
        """Abstract method.
        Checks if current page is not a valid business page
        (either an unavailable business page, or some other type of content)
        To be implemented by each scraper specifically for its site.
        Returns:
            True if not a business page,
            False otherwise
        """

        return False

    def stringify_children(self, node):
        '''Get all content of node, including markup.
        :param node: lxml node to get content of
        '''

        parts = ([node.text] +
                list(chain(*([etree.tostring(c, with_tail=False), c.tail] for c in node.getchildren()))) +
                [node.tail])
        # filter removes possible Nones in texts and tails
        return ''.join(filter(None, parts))

    def strip_tags(self, html):
        """
        Remove html tag from text
        """
        if isinstance(html, str):
            strHtml = html.replace('\/', '/').replace('</br>', '\n\n').replace('&amp;', '&')
            s = HTMLStripper()
            s.feed(strHtml)
            return s.get_data()
        return None

class HTMLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)
