"""
# Geodata

Export geo location data from the recent_changes table. The script is running multiple languages in parallel using the `multiprocessing` module. 

"""

import os,logging,argparse
from datetime import datetime,timedelta
from multiprocessing import Pool


import geo_coding as gc
import wikipedia_projects
import mysql_config


logger = logging.getLogger('process_data')


# data_dir = './data'
output_dir = None
timestamp = None
geoIP_db = None
n_threads = None


def main():
    '''
    
    '''
    p = Pool(n_threads)

    # languages = languages.languages
    # wp_projects =  ['ar','pt','hi','en']
    p.map(process_data, wp_projects)
    
    # test a project for debugging
    # process_data('ar')  

    logger.info('All projects done. Results are in %s'%(output_dir))





def mysql_resultset(wp_pr,ts=None):
    '''Returns an iterable MySql resultset using a server side cursor that can be used to iterate the data. Alternavively, the `dump_data_iterator()` method dumps the data onto disk before  aggregation. 
    '''
    # query = mysql_config.construct_rc_query(db_name)  
    query = mysql_config.construct_cu_query(wp_pr=wp_pr,ts=ts)
    logger.debug("SQL query for %s for ts=%s:\n\t%s"%(wp_pr,ts,query))

    cur = mysql_config.get_cursor(wp_pr,server_side=True)
    cur.execute(query)

    return cur


def retrieve_bot_list(wp_pr):
    '''Returns a set of all known bots for `wp_pr`. Bots are not labeled in a chohesive manner for Wikipedia. We use the union of the bots used for the [Wikipedia statistics](stats.wikimedia.org/), stored in `./data/erikZ.bots` and the `user_group.ug_group='bot'` flag in the MySql database. 
    ''' 

    erikZ_bots = set(long(b) for b in open('data/erikZ.bots','r'))

    query = mysql_config.construct_bot_query(wp_pr)
    cur = mysql_config.get_cursor(wp_pr,server_side=False)
    cur.execute(query)

    pr_bots = set(c[0] for c in cur)

    logger.debug("%s: There are %s additional bots (from %s) not in ErikZ bot file"%(wp_pr,len(pr_bots-erikZ_bots),len(pr_bots)))

    return erikZ_bots.union(pr_bots)

def dump_data_iterator(wp_pr,compressed=False):
    '''Dumps the needed entries from the recent changes table for project `wp_pr`. This is an alternative method to `mysql_resultset()` which retrieves the data directly from the server instead of dumping it to disk first.

    WARNING: Deprecated for now. E.g. Not working with ts.

    :returns: Iterable open file object 
    '''

    logger.warning("DEPRECATED!")

    # if not os.path.exists(data_dir):
    #   os.mkdir(data_dir)

    host_name = mysql_config.get_host_name(wp_pr)
    db_name = mysql_config.get_db_name(wp_pr)
    
    # mysql query to export recent changes data
    query = mysql_config.recentchanges_query%db_name


    if compressed:
        output_fn = os.path.join(data_dir,'%s_geo.tsv.gz'%wp_pr)    
        # export_command = ['mysql', '-h', host_name,  '-u%s'%user_name,  '-p%s'%pw ,'-e', "'%s'"%query, '|', 'gzip', '-c' ,'>', output_fn]
        export_command = ['mysql', '-h', host_name ,'-e', "'%s'"%query, '|', 'gzip', '-c' ,'>', output_fn]

    else:
        output_fn = os.path.join(data_dir,'%s_geo.tsv'%wp_pr)       
        # export_command = ['mysql', '-h', host_name,  '-u%s'%user_name,  '-p%s'%pw ,'-e', "'%s'"%query, '>', output_fn]
        export_command = ['mysql', '-h', host_name ,'-e', "'%s'"%query, '>', output_fn]


    # use problematic os.system instead of subprocess
    os.system(' '.join(export_command)) 

    if compressed:
        source =  gzip.open(output_fn, 'r')
    else:
        source =  open(output_fn, 'r')

    # discard the headers!
    source.readline()

    return source



def process_data(wp_pr):

    logger.info('CREATING DATASET FOR %s'%wp_pr)

    # EXTRACT

    ### export the data from mysql by dumping into a temp file
    # source = dump_data_iterator(wp_pr,compressed=True)        
    # (editors,countries_cities) = gc.extract(source=source,filter_id=(),sep='\t')
    
    # OR
    
    ### use a server-side cursor to iterate the result set
    source = mysql_resultset(wp_pr,ts=timestamp)
    bots = retrieve_bot_list(wp_pr) 
    (editors,cities) = gc.extract(source=source,filter_id=bots,geoIP_fn = geoIP_db)

    # TRANSFORM (only for editors)
    countries_editors,countries_cities = gc.transform(editors,cities)

    # LOAD 
    gc.load(wp_pr,countries_editors,countries_cities,output_dir=output_dir,ts=timestamp)

    # delete the exported data
    # os.system('rm %s'%fn)

    logger.info('Done : %s'%wp_pr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="""Geo coding editor activity on Wikipedia
        """
    )
    parser.add_argument(
        '-o', '--output',
        metavar='',
        type=str, 
        default='./output',
        dest = 'output_dir',
        help='<path> for output'
    )
    parser.add_argument(
        '-p', '--wp',
        metavar='',     
        nargs='+',      
        dest = 'wp_projects',
        default = [],
        help='the wiki project to analyze (e.g. `en`)',
    )
    parser.add_argument(
        '-t', '--timestamp',
        metavar='',
        type=str,
        default=datetime.strftime(datetime.now()-timedelta(days=1),'%Y%m%d'), 
        dest='timestamp',
        help="timestamp, '201205' or '20120518'. Default is yesterday."
    )
    parser.add_argument(
        '-n', '--threads',
        metavar='',
        type=int,
        default=2,  
        dest='threads', 
        help="number of threads (default=2)"
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        dest='verbose', 
        help="logging level"
    )
    parser.add_argument(
        '-g', '--geoDB',
        metavar='',
        type=str, 
        default='/usr/share/GeoIP/GeoIPCity.dat',
        dest = 'geoIP_db',
        help='<path> to geo IP database'
    )

    args = parser.parse_args()

    # setting loging configuration  
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,format='%(asctime)s (%(levelname)s) : %(message)s', datefmt='%m/%d/%y %H:%M:%S')  
    
    # check that output directory exists, create if not
    output_dir = args.output_dir    
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # check for mysql login credentials
    if not os.path.exists(os.path.expanduser("~/.my.cnf")):
        logger.error("~/.my.cnf does not exist! MySql connection might fail")
        pass


    wp_projects = wikipedia_projects.check_validity(args.wp_projects)   
    if not wp_projects:
        logging.error("No valid wikipedia projects.")

    geoIP_db = args.geoIP_db
    n_threads=args.threads
    timestamp=args.timestamp

    main()