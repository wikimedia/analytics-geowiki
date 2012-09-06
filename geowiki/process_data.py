"""
# Geodata

Export geo location data from the recent_changes table. The script is running multiple languages in parallel using the `multiprocessing` module. 

"""

import os,logging,argparse,pprint
import datetime, dateutil, dateutil.relativedelta
from multiprocessing import Pool
import functools


import geo_coding as gc
import wikipedia_projects
import mysql_config
import traceback


logger = logging.getLogger('process_data')


class AutoDate(datetime.date):
    def __init__(self, datestr):
        dt = dateutil.parser.parser(datestr)
        super(AutoDate,self).__init__(dt.date())

def run_parallel(args):
    '''
    Start `n_threads` processes that work through the list of projects `wp_projects`
    '''
    p = Pool(args.threads)

    # wp_projects =  ['ar','pt','hi','en']
    partial_process_project = functools.partial(process_project, args)
    p.map(partial_process_project, args.wp_projects)
    
    # test a project for debugging
    # process_project('ar')  

    logger.info('All projects done. Results are in %s'%(args.output_dir))


def mysql_resultset(wp_pr, start, end):
    '''Returns an iterable MySql resultset using a server side cursor that can be used to iterate the data. Alternavively, the `dump_data_iterator()` method dumps the data onto disk before  aggregation. 
    '''
    # query = mysql_config.construct_rc_query(db_name)  
    query = mysql_config.construct_cu_query(wp_pr=wp_pr,start=start, end=end)
    logger.debug("SQL query for %s for start=%s, end=%s:\n\t%s"%(wp_pr, start, end, query))

    cur = mysql_config.get_cursor(wp_pr,server_side=True)
    cur.execute(query)

    return cur


def retrieve_bot_list(wp_pr):
    '''Returns a set of all known bots for `wp_pr`. Bots are not labeled in a chohesive manner for Wikipedia. We use the union of the bots used for the [Wikipedia statistics](stats.wikimedia.org/), stored in `./data/erikZ.bots` and the `user_group.ug_group='bot'` flag in the MySql database. 
    '''     
    bot_fn = os.path.join(os.path.split(__file__)[0], 'data', 'erikZ.bots')    
    erikZ_bots = set(long(b) for b in open(bot_fn,'r'))

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



def process_project(args, wp_pr):

    try:
        logger.info('CREATING DATASET FOR %s'%wp_pr)

        ### use a server-side cursor to iterate the result set
        source = mysql_resultset(wp_pr,args.start, args.end)
        bots = retrieve_bot_list(wp_pr)
        logger.debug('about to extract editor info type(source)=%s' % (type(source)))
        (editors,cities) = gc.extract(source=source,filter_ids=bots,geoIP_db=args.geoIP_db)
        logger.debug('extracted editors using geo client')

        # TRANSFORM (only for editors)
        countries_editors,countries_cities = gc.transform(editors,cities)

        # LOAD 
        gc.load(wp_pr,countries_editors,countries_cities,args)

        logger.info('Done : %s'%wp_pr)
    except:
        """
        this is the function which the multiprocessing pool maps
        for some reason the tracebacks don't make their way back to the main process
        so it is best to print them out from within the process
        """
        logger.error(traceback.format_exc())
        raise


def parse_args():

    parser = argparse.ArgumentParser(
        description="""Geo coding editor activity on Wikipedia
        """
    )
    parser.add_argument(
        '-o', '--output',
        dest = 'output_dir',
        metavar='output_dir',
        default='./output',
        help='<path> for output'
    )
    parser.add_argument(
        '-b', '--basename',
        default='geo_editors',
        help='base output file name used in <BASENAME>_wp_pr_start_end.{json,tsv}'
    )
    parser.add_argument(
        '-p', '--wp',
        metavar='proj',     
        nargs='+',
#        required=True,
        dest = 'wp_projects',
        default = [],
        help='the wiki project to analyze (e.g. `en`)',
    )
    parser.add_argument(
        '-s', '--start',
        metavar='start_timestamp',
        type=AutoDate,
        default=None,
        dest='start',
        help="inclusive query start date. parses string with dateutil.parser.parse()"
    )
    parser.add_argument(
        '-e', '--end',
        metavar='',
        type=AutoDate,
        default=datetime.date.today() - datetime.timedelta(days=1),
        dest='end',
        help="inclusive query start date. parses string with dateutil.parser.parse()"
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

    # post processing
    args = parser.parse_args()
    if not args.start:
        args.start = args.end - dateutil.relativedelta.relativedelta(months=1)

    wp_projects = wikipedia_projects.check_validity(args.wp_projects)   
    if not wp_projects:
        logging.error("No valid wikipedia projects.")

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # check that output directory exists, create if not
    output_dir = args.output_dir    
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    # check for mysql login credentials
    if not os.path.exists(os.path.expanduser("~/.my.cnf")):
        logger.error("~/.my.cnf does not exist! MySql connection might fail")

    logger.info('args: %s', pprint.pformat(args.__dict__,indent=2))
    return args


def main():
    """Entry point for geo coding package
    """

    # setting loging configuration
    logging.basicConfig(level=logging.INFO,format='[%(levelname)s]\t[%(processName)s]\t[%(filename)s:%(lineno)d]\t[%(funcName)s]\t%(message)s')  
    
    args = parse_args()
    run_parallel(args)

if __name__ == '__main__':
    main()
