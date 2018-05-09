# -*- coding: utf-8 -*-
"""
Yelp API v2.0 code sample.

This program demonstrates the capability of the Yelp API version 2.0
by using the Search API to query for businesses by a search term and location,
and the Business API to query additional information about the top result
from the search query.

Please refer to http://www.yelp.com/developers/documentation for the API documentation.

This program requires the Python oauth2 library, which you can install via:
`pip install -r requirements.txt`.

Sample usage of the program:
cd ~/ETLEnv/DATA/yelp && source yelp/bin/activate
python yelp-api-v21-term-search.py --term="applebee's" --offset=0 -f postgres-test.csv 
"""
import argparse
import json
import pprint
import sys
import urllib
import urllib2
import csv

import oauth2


API_HOST = 'api.yelp.com'
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'San Francisco, CA'
SEARCH_LIMIT = 500
SEARCH_PATH = '/v2/search/'
BUSINESS_PATH = '/v2/business/'

# OAuth credential placeholders that must be filled in by users.
CONSUMER_KEY = "1"
CONSUMER_SECRET = "1"
TOKEN = "1"
TOKEN_SECRET = "1"


def request(host, path, url_params=None):
    """Prepares OAuth authentication and sends the request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        urllib2.HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = 'http://{0}{1}?'.format(host, path)

    consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
    oauth_request = oauth2.Request(method="GET", url=url, parameters=url_params)

    oauth_request.update(
        {
            'oauth_nonce': oauth2.generate_nonce(),
            'oauth_timestamp': oauth2.generate_timestamp(),
            'oauth_token': TOKEN,
            'oauth_consumer_key': CONSUMER_KEY
        }
    )
    token = oauth2.Token(TOKEN, TOKEN_SECRET)
    oauth_request.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
    signed_url = oauth_request.to_url()
    
    #print signed_url
    #print 'Querying {0} ...'.format(url)

    conn = urllib2.urlopen(signed_url, None)
    try:
        response = json.loads(conn.read())
    finally:
        conn.close()

    return response

def search(term, bounds , offset, limit):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """
    
    url_params = {
        'term': term.replace(' ', '+'),
        'bounds': bounds,#.replace(' ', '+'),
        'offset': offset,
        'limit': limit
    }
    return request(API_HOST, SEARCH_PATH, url_params=url_params)

def get_business(business_id):
    """Query the Business API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path)

def query_api( term, bounds, offset, limit):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    
    response = search(term, bounds, offset, limit)
    
    #Grab elements from JSON API response
    businesses = response.get('businesses')
    total = response.get('total')
    

    print "offset is " +str(offset)
        
    if total > 0:
        percent_complete = ((len(businesses) + offset  ) / total) *100
        print str(percent_complete) + "% of " + str(total) + " results"
    else:
        print "0 results"

    # not businesses:
        #print 'No businesses for {0} in {1} found.'.format(term, bounds)

    print ' - {0} businesses found'.format(
        len(businesses)
    ) + "- " + term +" - "+ bounds 

    if len(businesses) > 0:
        return businesses, total
    else:
        businesses = [] 
        total = 0
        return businesses, total


def getApiError(term, bounds, offset, limit):
    response = search(term, bounds, offset, limit)

    error = response.get('error')
    print error
    return error

def openCSV(file):
    """
    Arguments: 
        file (str): The file containing the boundaries of every U.S. city
    Returns: 
        data_rows (dict) : Every column is a key in the dict.
    """
    data_rows = []
    with open(file, 'rb') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';', quotechar="\"")
        for row in csv_reader:
            data_rows.append(row)
    return data_rows

def getLatLongFromRow(row):
    """
    Arguments: 
        row (dict): The row containing the dictionary with column names as keys
    Returns: 
        sw_latitude (float): The latitude of the row
    """ 
    latitude_sw = float(row['minlat'])
    latitude_ne = float(row['maxlat'])

    #longitudes do not change
    sw_longitude = float(row['minlon'])
    ne_longitude = float(row['maxlon'])

    geoid = str(row['geoid'])
    print geoid

    return latitude_sw, sw_longitude, latitude_ne, ne_longitude

def createBounds(sw_latitude, sw_longitude, ne_latitude, ne_longitude):
    """
    Arguments: 
        sw_lat (float): 
        sw_long (float): 
        ne_lat (float): 
        ne_long (float): 
    Returns: 
        bounds (str): string of 2 coordinates formatted for the API
    """ 
    bounds = str(sw_latitude) +','+ str(sw_longitude) + "|" + str(ne_latitude) + "," + str(ne_longitude)
    return bounds


def writeCSV(data, term, headers_fields=None):
    '''
    Arguments: 
        businesses (list) : The response object
        term (str) : Argument passed by user
        bounds (str) : Programmatically created from file
    Returns: 
        Writes to .csv
    '''

    file_path = 'yelp-biz-result-v9-'+ term+'.csv'

    if not headers_fields:
        headers_fields = data[0].keys()

    with open(file_path, 'a+') as csvfile:
        writer = csv.DictWriter(csvfile, headers_fields, delimiter='|')

        writer.writeheader()
        writer.writerows(data)


def formatResult(businesses, term, bounds):
    """ 
    Formats the results from the JSON response oject 

    Arguments: 
        businesses (list) : The response object
        term (str) : Argument passed by user
        bounds (str) : Programmatically created from file
    Returns: 
        header_row (list): List of strings of field names for .csv
        data (list): List of dicts. Each list item will create a row in the .csv
    """

    header_row = ['business_id','name','rating','review_count','address','url',
                'rating_img_url','categories','latitude','longitude','snippet_text',
                'image_url','is_closed']

    #check json response
    #print businesses

    ii = 0
    data = []
    for ii in range(len(businesses)):


        
        #If the JSON node exisits, add to our dict
        try:
            categories = businesses[ii]["categories"]
        except KeyError:
            categories = []
        categories_normalized = ""

        #Parse categories list
        for category in categories:

            to_add = category[1]
            categories_normalized = "{0}^{1}".format(categories_normalized,to_add)

        row_dict = {'business_id': businesses[ii]['id'],'name': businesses[ii]["name"],
                'rating':businesses[ii]["rating"],'review_count':businesses[ii]["review_count"]}        

        #If the JSON node exisits, add to our dict
        try:
            row_dict.update({'address':businesses[ii]["location"]["address"]})
        except KeyError:
            row_dict.update({'address':''})
        try:
            row_dict.update({'url':businesses[ii]["url"]})
        except KeyError:
            row_dict.update({'rating_img_url':businesses[ii]["rating_img_url"]})
        try:
            row_dict.update({'rating_img_url':''})
        except KeyError:
            row_dict.update({'rating_img_url':''})            
        try:
            row_dict.update({'categories': categories_normalized})
        except KeyError:
            print "null data" 
        try:
            row_dict.update({'latitude':businesses[ii]["location"]["coordinate"]['latitude']})            
        except KeyError:
            print "null data"             
        try:
            row_dict.update({'longitude':businesses[ii]["location"]["coordinate"]['longitude']})
        except KeyError:
            print "null data"             
        try:
            row_dict.update({'snippet_text':businesses[ii]['snippet_text'].replace('\n','***')})
        except KeyError:
            print "null data"             
        try:
            row_dict.update({'image_url':businesses[ii]['image_url']})
        except KeyError:
            row_dict.update({'image_url':''})

        try:
            row_dict.update({'is_closed':businesses[ii]['is_closed']})
        except KeyError:
            row_dict.update({'is_closed':''})

        #encode all fields before passing to .csv writer
        for key in row_dict:
            row_dict[key] = unicode(row_dict[key]).encode('utf-8')

        data.append(row_dict)

    return header_row, data

def CalcBoundsFromLatLong(latitude_sw, sw_longitude, latitude_ne, ne_longitude, num_divisions):
    """
    Arguments: 
        latitude_sw (float): the minimum latitude which will be parsed into smaller divisions
    Returns: 
        all_slices (list): A list of dicts containing the sw_latitude, sw_longitude, 
            ne_latitude, ne_longitude of the smaller slices
    """ 

    #Find total latitude and longitude distance of city
    span_lat = latitude_ne - latitude_sw ;
    span_long = ne_longitude - sw_longitude;

    all_slices = []
    i = 0
    print "i divided this geo into " + str(num_divisions)
    while i < num_divisions:

        ne_latitude = latitude_sw + (span_lat/num_divisions)* (i+1);

        sw_latitude =  latitude_sw+ (span_lat/num_divisions)*i;

        bounds = str(sw_latitude) +','+ str(sw_longitude) + "|" + str(ne_latitude) + "," + str(ne_longitude)
        
        # Put SW lat/long, NE lat/long and bounds (str) into dictionary
        slice_dict = {'sw_latitude': sw_latitude, 'sw_longitude': sw_longitude,'ne_latitude': ne_latitude,'ne_longitude': ne_longitude,'bounds': bounds }
        
        all_slices.append(slice_dict)

        i = i + 1

    #return bounds_per_city
    return all_slices

def queryAllResults(sw_latitude, sw_longitude, ne_latitude, ne_longitude, term, offset, limit):
    """
    Arguments: 
        city (dict): a row of the .csv
        term (str): Yelp API parameter - the Yelp category used for this search Ex. breweries
        offset (int): Yelp API parameter - the offset used for the query
        limit (int): Yelp API parameter - the reuslt limit
    Returns: 

    """

    bounds = createBounds(sw_latitude, sw_longitude, ne_latitude, ne_longitude)


    try:
        response, total= query_api(term, bounds, offset, limit)
        #print response
        print "total businesses is ---- " +str(total)
    except urllib2.HTTPError as error:
        #error = getApiError(term, bounds, offset, limit)
        sys.exit('Encountered HTTP error {0}. Abort program.'.format(error.code))
    if total > 1000:
        repartitionGeo(sw_latitude, sw_longitude, ne_latitude, ne_longitude, 
                        term, offset, limit, 1)
        print "exiting function"
        return 0
    header_row, data = formatResult(response, term, bounds)
    writeCSV(data, term, header_row)
    if len(response) == limit:
        offset = offset + limit - 1
        print "querying"
        queryAllResults(sw_latitude, sw_longitude, ne_latitude, ne_longitude, 
                        term, offset, limit)
    else:
        print "reached last page of results"
        return 1

def repartitionGeo(sw_latitude, sw_longitude, ne_latitude, ne_longitude, 
                    term, offset, limit, num_divisions=1):
    count = 0

    num_divisions = num_divisions + 1
    all_bounds = CalcBoundsFromLatLong(sw_latitude, sw_longitude, ne_latitude, ne_longitude, num_divisions)

    for bound in all_bounds:
        count = count + 1
        print "querying the #" + str(count) + " slice of a city"

        #the right way to do it recursively
        queryAllResults(bound['sw_latitude'], bound['sw_longitude'], 
                        bound['ne_latitude'], bound['ne_longitude'], 
                        term, 0 , limit) 
    return 1

def main():
    parser = argparse.ArgumentParser()

    #search by term
    parser.add_argument('-t', '--term', dest='term', 
                        default=DEFAULT_TERM, type=str, 
                        help='Search term (default: %(default)s)')

    parser.add_argument('-o', '--offset', dest='offset',
                        default=0, type=int,
                        help='Offset (default: %(default)s)')
    parser.add_argument('-x', '--limit', dest='limit',
                        default=SEARCH_LIMIT, type=int,
                        help='Number of results (default: %(default)s) results')
    parser.add_argument('-f', '--filename', dest='filename',
                        default="file.csv", type=str,
                        help='Name of the file containing the city coordinates')

    input_values = parser.parse_args()

    filerows = openCSV(input_values.filename)

    #iterate through a file with a row for each U.S. city
    for city in filerows:
        #Get the coordinates from the file

        sw_latitude, sw_longitude, ne_latitude, ne_longitude = getLatLongFromRow(city)

        #pass coordinates to pull data from   
        queryAllResults(sw_latitude, sw_longitude, ne_latitude, ne_longitude, input_values.term, input_values.offset, input_values.limit)


if __name__ == '__main__':
    main()






