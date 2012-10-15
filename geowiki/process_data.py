"""
# Geodata

Export geo location data from the recent_changes table. The script is running multiple languages in parallel using the `multiprocessing` module. 

"""

import os,sys, logging,argparse,pprint
import datetime, dateutil.relativedelta, dateutil.parser
from multiprocessing import Pool
import functools, copy
from operator import itemgetter

import geo_coding as gc
import wikipedia_projects
import mysql_config
import traceback


logger = logging.getLogger('process_data')
log_fmt = logging.Formatter('[%(levelname)s]\t[%(processName)s]\t[%(filename)s:%(lineno)d]\t[%(funcName)s]\t%(message)s')
logger.setLevel(logging.DEBUG)

def run_parallel(args):
    '''
    Start `n_threads` processes that work through the list of projects `wp_projects`
    '''
    p = Pool(args.threads)

    # wp_projects =  ['ar','pt','hi','en']
    partial_process_project = functools.partial(process_project, args)
    p.map(partial_process_project, args.wp_projects)
    
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
        (editors,cities) = gc.extract(source=source,filter_ids=bots,geoIP_db=args.geoIP_db)

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

    class WPFileAction(argparse.Action):
        """
        This action is fired upon parsing the --wpfiles option which should be a list of 
        tsv file names.  Each named file should have the wp project codes as the first column
        The codes will be used to query the databse with the name <ID>wiki.
        
        (Sorry about the nasty python functional syntax.)
        """
        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, self.dest, list(set(
                        map(
                            itemgetter(0),
                            map(
                                str.split,
                                filter(
                                    lambda line: line[0] != '#',
                                    reduce(
                                        list.__add__, 
                                        map(
                                            file.readlines,
                                            map(
                                                open,
                                                values)), [])))))))


    def auto_date(datestr):
        #logging.debug('entering autodate: %s', datestr)
        return dateutil.parser.parse(datestr).date()

    parser = argparse.ArgumentParser(
        description="""Geo coding editor activity on Wikipedia""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-o', '--output',
        dest = 'output_dir',
        metavar='output_dir',
        default='./output',
        help='<path> for output.  program will actually make a subdirectory within the'
        'provided directory named according to the start and end date'
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
        dest = 'wp_projects',
        default = [],
        help='the wiki project to analyze (e.g. `en`)',
    )
    parser.add_argument(
        '--wpfiles',
        metavar='wp_ids.tsv',
        nargs='+',
        dest = 'wp_projects',
        action=WPFileAction,
        help='list of tsv files in which the first column is the project id and the second column is the full name'
        'will clobber any arguments passed in by --wp if --wpfiles appears later in list'
    )
    parser.add_argument(
        '-s', '--start',
        metavar='start_timestamp',
        type=auto_date,
        default=None,
        dest='start',
        help="inclusive query start date. parses string with dateutil.parser.parse()"
    )
    parser.add_argument(
        '-e', '--end',
        metavar='',
        type=auto_date,
        default=datetime.date.today() - datetime.timedelta(days=1),
        dest='end',
        help="inclusive query start date. parses string with dateutil.parser.parse()"
    )
    parser.add_argument(
        '--daily',
        action='store_true',
        default=False,
        help='including this flag instructs the program to run the query for each day between the '
        'start and end date (starting on each day and ending 30 days later) instead of only once '
        'for the entire range'
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
        '-q', '--quiet',
        action='store_true',
        help="set logging level to INFO rather than DEBUG"
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

    cu_start = datetime.date.today() - datetime.timedelta(days=90)
    if args.daily and args.start < cu_start + datetime.timedelta(days=30):
        parser.error('starting date (%s) exceeds persistence of check_user table (90 days, i.e. %s)' % (args.start, cu_start))

    wp_projects = wikipedia_projects.check_validity(args.wp_projects)   
    if not wp_projects:
        parser.error('error: no valid wikipedia projects recieved\n'
                         '       must either include the --wp flag or the --wpfiles flag\n')
    
    if args.quiet:
        logger.setLevel(logging.INFO)

    # create top-level dir
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    args.subdir = '%s_%s' % (datetime.date.strftime(args.start,'%Y%m%d'), 
                              datetime.date.strftime(args.end,'%Y%m%d'))

    # check for mysql login credentials
    if not os.path.exists(os.path.expanduser("~/.my.cnf")):
        logger.error("~/.my.cnf does not exist! MySql connection might fail")

    logger.info('args: %s', pprint.pformat(args.__dict__,indent=2))
    return args


def main():
    """Entry point for geo coding package
    """

    args = parse_args()
    if args.daily:
        orig_start = copy.deepcopy(args.start)
        orig_end = copy.deepcopy(args.end)
        for day in [orig_start + datetime.timedelta(days=n) for n in range((orig_end - orig_start).days)]:
            args.start = day - datetime.timedelta(days=30)
            args.end = day
            # give each run its own dir
            args.subdir = './%s_%s' % (datetime.date.strftime(args.start,'%Y%m%d'), 
                                  datetime.date.strftime(args.end,'%Y%m%d'))

            if not os.path.exists(os.path.join(args.output_dir,  args.subdir)):
                os.makedirs(os.path.join(args.output_dir, args.subdir))
            # log to file in subdir
            fh = logging.FileHandler(os.path.join(args.output_dir, args.subdir, 'log'))
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(log_fmt)
            logger.addHandler(fh)

            logging.info('running daily with args: %s', pprint.pformat(args.__dict__, indent=2))

            run_parallel(args)
    else:
        if not os.path.exists(os.path.join(args.output_dir,  args.subdir)):
            os.makedirs(os.path.join(args.output_dir, args.subdir))
        logger.addHandler(logging.FileHandler(os.path.join(args.output_dir, args.subdir, 'log')))

        run_parallel(args)

if __name__ == '__main__':
    try:
        main()
        print 2
    except:
        logging.error(traceback.format_exc())
        raise
