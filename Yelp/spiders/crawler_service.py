# -*- coding: utf-8 -*-

# !/usr/bin/python
import os
import datetime
import json
import re
import xlrd
from urllib.request import HTTPError

from flask import jsonify, request, current_app

from spiders import spider
from spiders.yelp.extract_yelp_data import YelpScraper
from config.get_config import Config

base_dir_path = os.path.dirname(os.path.realpath(__file__))
ENV = os.getenv('ENV') or 'development'
cfg = Config(ENV)

# dictionary containing supported sites as keys
# and their respective scrapers as values
SUPPORTED_SITES = {
    "www.yelp.com": YelpScraper
}


class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['error'] = self.message
        return rv


class GatewayError(Exception):
    status_code = 502

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['error'] = self.message
        return rv


# validate input and raise exception with message for client if necessary
def check_input(url, is_valid_url, invalid_url_message=""):
    # TODO: complete these error messages with more details specific to the scraped site
    if not url:
        raise InvalidUsage("No input URL was provided.", 400)

    if not is_valid_url:
        try:
            error_message = "Invalid URL: " + str(url) + " " + str(invalid_url_message)
        except UnicodeEncodeError:
            error_message = "Invalid URL: " + url.encode("utf-8") + str(invalid_url_message)
        raise InvalidUsage(error_message, 400)


# infer domain from input URL
def extract_domain(url):
    '''
    if 'www.yelp.com' in url:
        # for yelp scraper
        # https://www.yelp.com/biz/holland-gary-g-md-seminole
        return 'yelp'
    if 'www.vitals.com' in url:
        # for vitals scraper
        # https://www.vitals.com/doctors/Dr_Nadia_Sadek.html
        return 'vitals'
    if 'www.google.com' in url:
        # for google scraper
        # https://www.google.com/search?ei=V_T1W6idN8iy8QWq1rSIDg&q=Ocala+Family+Medical+Center&oq=Ocala+Family+Medical+Center&gs_l=psy-ab.12..0l10.200975.200975..201371...0.0..0.141.141.0j1......0....1j2..gws-wiz.......0i71.wtLTfmx1Qd4#lrd=0x88e7d4ddf16c4825:0xd8c7e915e7ec96c0,1,,,
        return 'google'
    if 'www.healthgrades.com' in url:
        # for healthgrades scraper
        # https://www.healthgrades.com/physician/dr-ailis-marrero-y7l83
        return 'healthgrades'
    '''

    m = re.match("^https?://(www|shop|www1|intl)\.([^/\.]+)\..*$", url)

    if m:
        return m.group(2)
    # TODO: return error message about bad URL if it does not match the regex


def recheck_json(data):
    photos_key = data.get('photos')
    for key in photos_key:
        try:
            img_url = key['url']
            img_desc = key['description']
            key['descriptions'] = key['url']
            del key['url']
            key['urls'] = key['description']
            del key['description']
            key['url'] = key['urls']
            del key['urls']
            key['url'] = img_url
            key['description'] = key['descriptions']
            key['description'] = img_desc
            del key['descriptions']

        except:
            pass
    return data

# validate request mandatory arguments
def validate_args(arguments):
    # normalize all arguments to str
    argument_keys = map(lambda s: str(s), arguments.keys())

    mandatory_keys = ['url']

    # If missing any of the needed arguments, throw exception
    for argument in mandatory_keys:
        if argument not in argument_keys:
            raise InvalidUsage("Invalid usage: missing GET parameter: " + argument)

    # Validate site
    # If no "site" argument was provided, infer it from the URL
    if 'site' in arguments:
        site_argument = arguments['site'][0]
    else:
        site_argument = extract_domain(arguments['url'][0])

        # If site could not be extracted the URL was invalid
        if not site_argument:
            raise InvalidUsage("Invalid input URL: " + arguments['url'][0] + ". Domain could not be extracted")

        # Add the extracted site to the arguments list (to be used in get_data)
        arguments['site'] = [site_argument]

    if site_argument not in SUPPORTED_SITES.keys():
        raise InvalidUsage("Unsupported site: " + site_argument)


# validate request "data" parameters
def validate_data_params(arguments, ALL_DATA_TYPES):
    # Validate data

    if 'data' in arguments:
        # TODO: do the arguments need to be flattened?
        data_argument_values = map(lambda s: str(s), arguments['data'])
        data_permitted_values = map(lambda s: str(s), ALL_DATA_TYPES.keys())

        # if there are other keys besides "data" or other values outside of the predefined data types (DATA_TYPES), return invalid usage
        if set(data_argument_values).difference(set(data_permitted_values)):
            # TODO:
            #      improve formatting of this message
            raise InvalidUsage("Invalid usage: Request arguments must be of the form '?url=<url>?site=<site>?data=<data_1>&data=<data_2>&data=<data_2>...,\n \
                with the <data_i> values among the following keywords: \n" + str(data_permitted_values))


# general resource for getting data.
# needs "url" and "site" parameters. optional parameter: "data"
# can be used without "data" parameter, in which case it will return all data
# or with arguments like "data=<data_type1>&data=<data_type2>..." in which case it will return the specified data
# the <data_type> values must be among the keys of DATA_TYPES imported dictionary
@spider.route('/get_data', methods=['GET'])
def get_data():
    # this is used to convert an ImmutableMultiDictionary into a regular dictionary. will be left with only one "data" key
    request_arguments = dict(request.args)

    # validate request parameters
    validate_args(request_arguments)

    url = request_arguments['url'][0]
    site = request_arguments['site'][0]

    if 'category' in request_arguments:
        category = request_arguments['category'][0]
    else:
        category = None

    if 'url2' in request_arguments:
        url2 = request_arguments['url2'][0]
    else:
        url2 = None

    if 'bot' in request_arguments:
        bot = request_arguments['bot'][0]
    else:
        bot = None

    if cfg.Proxy:
        proxies = {
            'host': cfg.Proxy["host"],
            'port': cfg.Proxy["port"],
            'apikey': cfg.Proxy["apikey"]
        }

        # create scraper class for requested site
        site_scraper = SUPPORTED_SITES[site](
            url=url,
            bot=bot,
            category=category,
            url2=url2,
            proxies=proxies
        )
    else:
        # create scraper class for requested site
        site_scraper = SUPPORTED_SITES[site](
            url=url,
            bot=bot,
            category=category,
            url2=url2
        )

    # for some special url, we need rebuild it
    site_scraper.rebuild_business_url()
    # validate parameter values
    # url
    is_valid_url = site_scraper.check_url_format()
    if hasattr(site_scraper, "INVALID_URL_MESSAGE"):
        check_input(url, is_valid_url, site_scraper.INVALID_URL_MESSAGE)
    else:
        check_input(url, is_valid_url)

    # data
    validate_data_params(request_arguments, site_scraper.ALL_DATA_TYPES)

    # return all data if there are no "data" parameters
    if 'data' not in request_arguments:
        try:
            ret = site_scraper.business_info()

        except HTTPError as ex:
            raise GatewayError("Error communicating with site crawled.")

        return jsonify(ret)

    # return only requested data
    try:
        ret = site_scraper.business_info(request_arguments['data'])
    except HTTPError:
        raise GatewayError("Error communicating with site crawled.")

    return jsonify(ret)


@spider.route('/run_batch', methods=['GET'])
def run_batch():
    # this is used to convert an ImmutableMultiDictionary into a regular dictionary. will be left with only one "data" key
    request_arguments = dict(request.args)
    site = request_arguments['site'][0]
    import os
    json_result_list = []
    file = xlrd.open_workbook(base_dir_path + "/Research Offices.xlsx")
    sheet = file.sheet_by_index(0)

    for row in range(1, sheet.nrows):
        print(len(sheet.row_values(row)[9]), sheet.row_values(row)[9])
        if str(sheet.row_values(row)[9]):
            if cfg.Proxy:
                proxies = {
                    'host': cfg.Proxy["host"],
                    'port': cfg.Proxy["port"],
                    'apikey': cfg.Proxy["apikey"]
                }

                # create scraper class for requested site
                site_scraper = SUPPORTED_SITES[site](
                    url=str(sheet.row_values(row)[9]),
                    bot=None,
                    category=None,
                    url2=None,
                    proxies=proxies
                )
            else:
                # create scraper class for requested site
                site_scraper = SUPPORTED_SITES[site](
                    url=str(sheet.row_values(row)[9]),
                    bot=None,
                    category=None,
                    url2=None,
                )

            # for some special url, we need rebuild it
            site_scraper.rebuild_business_url()
            # validate parameter values
            # url
            is_valid_url = site_scraper.check_url_format()
            if hasattr(site_scraper, "INVALID_URL_MESSAGE"):
                check_input(str(sheet.row_values(row)[9]), is_valid_url, site_scraper.INVALID_URL_MESSAGE)
            else:
                check_input(str(sheet.row_values(row)[9]), is_valid_url)

            # data
            validate_data_params(request_arguments, site_scraper.ALL_DATA_TYPES)

            # return all data if there are no "data" parameters
            if 'data' not in request_arguments:
                try:
                    ret = site_scraper.business_info()
                    ret["external_system_unique_id"] = int(sheet.row_values(row)[0])
                except HTTPError as ex:
                    raise GatewayError("Error communicating with site crawled.")

                import json
                # print("*************** SAVE FILE ******************       ")
                output_file_path = os.path.dirname(os.path.abspath(__file__)) + "/output/business-{}.json".format(
                    int(sheet.row_values(row)[0]))
                # print("FILE PATH       ",output_file_path)
                with open(output_file_path, 'w') as outfile:
                    ret = recheck_json(ret)
                    json.dump(ret, outfile, indent=4)
                    outfile.close()

                json_result_list.append(ret)

    return jsonify(json_result_list)


@spider.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    # TODO: not leave this as json output? error format should be consistent
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@spider.errorhandler(GatewayError)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@spider.errorhandler(404)
def handle_not_found(error):
    response = jsonify({"error": "Not found"})
    response.status_code = 404
    return response


@spider.errorhandler(500)
def handle_internal_error(error):
    response = jsonify({"error": "Internal server error"})
    response.status_code = 500
    return response


# post request logger
@spider.after_request
def post_request_logging(response):
    current_app.logger.info(json.dumps({
        "date": datetime.datetime.today().ctime(),
        "remote_addr": request.remote_addr,
        "request_method": request.method,
        "request_url": request.url,
        "response_status_code": str(response.status_code),
        "request_headers": ', '.join([': '.join(x) for x in request.headers])
    })
    )

    return response
