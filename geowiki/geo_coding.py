'''

ETL for geo coding entries from the recentchanges table. 

1. Country, total editors, total active editors (5+), total very active editors (100+)
2. Country, top 10 cities, percentage of total edits from each city  


'''

import sys,os,logging
import datetime
import gzip,re
import operator
import mysql_config
import pprint
from collections import defaultdict

import pygeoip

logger = logging.getLogger(__name__)

def valid_ip(ip):
    return bool(re.match('[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}',ip))

### EXTRACT
def extract(source,filter_ids,geoIP_db,sep=None):
    '''Extracts geo data on editor and country/city level from the data source.

    The source is a compressed mysql result set with the following format.

    for s in source:
        s[0] == user_name
        s[1] == ip address
        s[2] == len changed

    :arg source: iterable
    :arg filter_ids: set, containing user id that should be filtered, e.g. bots. Set can be empty in which case nothing will be filtered.
    :arg geoIP_db: str, path to Geo IP database
    :arg sep: str, separator for elements in source if they are strings. If None, elements won't be split
    :returns: (editors,cities)
    '''
    logger.debug('entering, geoIP_db: %s' % (geoIP_db))
    gi = pygeoip.GeoIP(geoIP_db, pygeoip.const.MEMORY_CACHE)
    #gi = pygeoip.GeoIP(geoIP_db)
    logger.debug('loaded cache')

    # test
    #logger.debug(gi.record_by_addr('178.192.86.113'))
    #logger.debug('passes test')

    editors = {}
    cities = {}

    for line in source:
        # a line can be a tuple from a sql resultset or a '\n' escaped line in a text file
        if sep:
            res = line[:-1].split(sep)
        else:
            res = line
        
        user = res[0]

        # filter!
        if user in filter_ids:
            continue

        ip = res[1]

        # geo lookup
        record = None
        if valid_ip(ip):
            try:
                record = gi.record_by_addr(ip)
            except:
                logger.exception('encountered exception while geocoding ip: %s', ip)
                continue
        else:
            # ip invalid
            city = 'Invalid IP'
            country = 'Invalid IP'

        if record:
            city = record['city'] 
            country = record['country_name'] 
            
            if city=='' or city==' ':
                city = "Unknown"

            if country=='' or country==' ':
                country = "Unknown"
        else:
            # ip invalid
            city = 'Invalid IP'
            country = 'Invalid IP'




        # country -> city data
        if country not in cities:
            cities[country] = {}

        if city in cities[country]:
            cities[country][city] += 1
        else:
            cities[country][city] = 1

        
        # country -> editors data

        if user not in editors:
            editors[user] = {}

        if country in editors[user]:
            editors[user][country]['edits'] += 1
            # editors[user][country]['len_change'] += len_change
        else:
            # country not in editors[user]
            editors[user][country] = {}
            editors[user][country]['edits'] = 1
            # editors[user][country]['len_change'] = len_change

    return (editors,cities)



def get_active_editors(wp_pr, editors, opts):
    ### Editor activity

    editor_counts = defaultdict(lambda : defaultdict(int))
    
    bins = map(str,range(1,11))
    bins = bins + ['%d-%d' % (thresh, thresh + 10) for thresh in range(0,100,10)]
    bins = bins + ['all', '5+', '100+']
    init_cohorts = dict(zip(bins, [0]*len(bins)))

    country_nest = defaultdict(lambda: defaultdict(int))
    world_nest = defaultdict(int)

    for editor, ginfo in editors.iteritems():
        for country, einfo in ginfo.iteritems():
            count = einfo['edits']
            if count > 0:
                country_nest[country]["all"] +=1
                world_nest["all"] += 1
                if count >= 5:
                    country_nest[country]["5+"] +=1                 
                    world_nest["5+"] += 1
                    if count >= 100:
                        country_nest[country]["100+"] +=1 
                        world_nest["100+"] += 1
            if count <= 10:
                country_nest[country]['%d' % count] += 1
            if count < 100:
                bottom = 10 * (int(count) / 10)
                country_nest[country]['%s-%s' % (bottom, bottom + 10)] += 1
                

    #flatten
    country_rows = []
    start_str = opts['start'].isoformat()
    end_str = opts['end'].isoformat()
    for country, cohorts in country_nest.items():
        for cohort, count in cohorts.items():
            row = {'project' : wp_pr,
                   'country' : country,
                   'cohort' : cohort,
                   'start' : start_str,
                   'end' : end_str,
                   'count' : count}
            country_rows.append(row)

    world_rows = []
    for cohort, count in world_nest.items():
        row = {'project' : wp_pr,
               'cohort' : cohort,
               'start' : start_str,
               'end' : end_str,
               'count' : count}
        world_rows.append(row)

    return country_rows, world_rows



def get_city_edits(wp_pr, countries, opts):
    ### City rankings

    city_rows = []
    country_totals = []
    start_str = opts['start'].isoformat()
    end_str = opts['end'].isoformat()
    for country,cities in countries.iteritems():
        
        city_info_sorted = sorted(cities.iteritems(),key=operator.itemgetter(1),reverse=True)   
        totaledits = sum([c[1] for c in city_info_sorted])
        row = {'project' : wp_pr,
               'country' : country,
               'start' : start_str,
               'end' : end_str,
               'edits' : totaledits}
        country_totals.append(row)

        ### pseudo-confuscation for 1 to 10 scale
        #city_info_sorted_aggr = [ (c[0] , (10.*c[1]/city_info_sorted[0][1])) for c in city_info_sorted[:opts['top_cities']]]

        # normalization
        city_info_normalized = [(name, edits / float(totaledits)) for (name, edits) in city_info_sorted]
        city_info_min_fraction = filter(lambda (name, frac) : frac >= 0.1, city_info_normalized)
        for city, frac in city_info_min_fraction:
            row = {'project' : wp_pr,
                   'country' : country,
                   'city' : city,
                   'start' : start_str,
                   'end' : end_str,
                   'fraction' : frac}
            city_rows.append(row)

    return city_rows, country_totals

