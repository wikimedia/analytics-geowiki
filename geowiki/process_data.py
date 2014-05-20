#!/usr/bin/python

"""
# Geodata

Export geo location data from the recent_changes table. The script is running
multiple languages in parallel using the `multiprocessing` module.

"""

import argparse
import copy
import datetime
import dateutil.parser
import dateutil.relativedelta
import functools
import logging
import os
import pprint

from multiprocessing import Pool
from operator import itemgetter

import geo_coding as gc
import wikipedia_projects
import mysql_config
import traceback


root_logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(name)s]\t[%(levelname)s]\t[%(processName)s]\t[%(filename)s:%(lineno)d]\t[%(funcName)s]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)


def run_parallel(opts):
    '''
    Start `opts['threads']` processes that work through the list of projects `wp_projects`
    '''
    p = Pool(opts['threads'])

    # wp_projects =  ['ar','pt','hi','en']
    partial_process_project = functools.partial(process_project, opts=opts)
    p.map(partial_process_project, opts['wp_projects'])

    logger.info('All projects done. Results are in %s' % (opts['output_dir']))


def mysql_resultset(wp_pr, start, end, opts):
    '''
    Returns an iterable MySql resultset using a server side cursor that can be
    used to iterate the data. Alternavively, the `dump_data_iterator()` method
    dumps the data onto disk before  aggregation.
    '''
    # query = mysql_config.construct_rc_query(db_name)
    query = mysql_config.construct_cu_query(wp_pr=wp_pr, start=start, end=end)
    logger.debug("SQL query for %s for start=%s, end=%s:\n\t%s" % (wp_pr, start, end, query))

    cur = mysql_config.get_analytics_cursor(wp_pr, opts, server_side=True)
    cur.execute(query)

    return cur


def retrieve_bot_list(wp_pr, opts):
    '''
    Returns a set of all known bots for `wp_pr`. Bots are not labeled in a
    chohesive manner for Wikipedia. We use the union of the bots used for the
    [Wikipedia statistics](stats.wikimedia.org/), stored in `./data/erikZ.bots`
    and the `user_group.ug_group='bot'` flag in the MySql database.
    '''
    bot_fn = os.path.join(os.path.split(__file__)[0], 'data', 'erikZ.bots')
    erikZ_bots = set(long(b) for b in open(bot_fn, 'r'))

    query = mysql_config.construct_bot_query(wp_pr)
    cur = mysql_config.get_analytics_cursor(wp_pr, opts, server_side=False)
    cur.execute(query)
    cur.connection.close()

    pr_bots = set(c[0] for c in cur)

    logger.debug("%s: There are %s additional bots (from %s) not in ErikZ bot file" % (
        wp_pr, len(pr_bots - erikZ_bots), len(pr_bots)))

    return erikZ_bots.union(pr_bots)


def process_project(wp_pr, opts):

    try:
        logger.info('CREATING DATASET FOR %s' % wp_pr)

        ### use a server-side cursor to iterate the result set
        source = mysql_resultset(wp_pr, opts['start'], opts['end'], opts)
        bots = retrieve_bot_list(wp_pr, opts)
        (editors, cities) = gc.extract(source=source, filter_ids=bots, geoIP_db=opts['geoIP_db'])

        # aggregate
        logging.debug('tallying')
        country_active_editors, world_active_editors = gc.get_active_editors(wp_pr, editors, opts)
        city_fractions, country_total_edits = gc.get_city_edits(wp_pr, cities, opts)

        # write to db
        logging.debug('writing to db')
        cursor = mysql_config.get_dest_cursor(opts)
        mysql_config.write_country_active_editors_mysql(country_active_editors, opts, cursor=cursor)
        mysql_config.write_world_active_editors_mysql(country_active_editors, opts, cursor=cursor)
        mysql_config.write_city_edit_fraction_mysql(city_fractions, opts, cursor=cursor)
        mysql_config.write_country_total_edits_mysql(country_total_edits, opts, cursor=cursor)
        cursor.close()

        # write files
        logging.debug('writing to files')
        #mysql_config.dump_json(wp_pr, 'country_active_editors', country_active_editors, opts)
        #mysql_config.dump_json(wp_pr, 'world_active_editors', world_active_editors, opts)
        #mysql_config.dump_json(wp_pr, 'city_fractions', city_fractions, opts)
        #mysql_config.dump_json(wp_pr, 'country_total_edits', country_total_edits, opts)

        logger.info('Done : %s' % wp_pr)
    except:
        """
        this is the function which the multiprocessing pool maps
        for some reason the tracebacks don't make their way back to the main process
        so it is best to print them out from within the process
        """
        logger.exception('caught exception within process:')
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

            # hack because moka flatten is broken
            #def flatten(l):
                #return moka.List(reduce(moka.List.extend, l, moka.List()))
            #moka.List.flatten = flatten

            #logging.info('values: %s', values)
            #projects = moka.List(values)\
                #.map(open)\
                #.map(file.readlines)\
                #.flatten()\
                #.keep(lambda line : line[0] != '#')\
                #.map(str.split)\
                #.map(itemgetter(0))\
                #.uniq()
            #project_list = list(projects)

            # logging.info('moka projects: %s', projects)

            projects = list(set(
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
                                        values)), []))))))

            # logging.info('new - old: %s', set(project_list) - set(old))
            # logging.info('old - new: %s', set(old) - set(project_list))
            # import sys
            # sys.exit()
            setattr(namespace, self.dest, projects)

    def auto_date(datestr):
        #logger.debug('entering autodate: %s', datestr)
        return dateutil.parser.parse(datestr).date()

    parser = argparse.ArgumentParser(
        description="""Geo coding editor activity on Wikipedia""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-o', '--output',
        dest='output_dir',
        metavar='output_dir',
        default='./output',
        help='<path> for output.  program will actually make a subdirectory within the'
        'provided directory named according to the start and end date'
    )
    parser.add_argument(
        '-b', '--basename',
        default='geowiki',
        help='base output file name used in <BASENAME>_wp_pr_start_end.{json,tsv}'
    )
    parser.add_argument(
        '-p', '--wp',
        metavar='proj',
        nargs='+',
        dest='wp_projects',
        default=[],
        help='the wiki project to analyze (e.g. `en`)',
    )
    parser.add_argument(
        '--wpfiles',
        metavar='wp_ids.tsv',
        nargs='+',
        dest='wp_projects',
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
        help="inclusive query start date. parses string with dateutil.parser.parse().  Note that if only "
        "a date is given, the hours, minutes and seconds will be filled in with 0's"
    )
    parser.add_argument(
        '-e', '--end',
        metavar='end_timestamp',
        type=auto_date,
        default=datetime.date.today() - datetime.timedelta(days=1),
        dest='end',
        help="exclusive query end date. parses string with dateutil.parser.parse()"
    )
    parser.add_argument(
        '--daily',
        action='store_true',
        default=False,
        help='including this flag instructs the program to run monthly queries ending on each day between the '
        'start and end date instead of only once for the entire range'
    )
    parser.add_argument(
        '-n', '--threads',
        metavar='',
        type=int,
        dest='threads',
        help="number of threads"
    )
    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help="set logging level to INFO rather than DEBUG"
    )
    parser.add_argument(
        '-g', '--geoDB',
        metavar='',
        default='/usr/share/GeoIP/GeoIPCity.dat',
        #default='/home/erosen/share/GeoIP/GeoIPCity.dat', # this one I manually manage
        dest='geoIP_db',
        help='<path> to geo IP database'
    )
    parser.add_argument(
        '--top_cities',
        type=int,
        default=10,
        help='number of cities to report when aggregating by city'
    )
    parser.add_argument(
        '--source_sql_cnf',
        type=os.path.expanduser,
        default='~/.my.cnf',
        help='mysql ini-style option file which allows a user to write to the read from database'
        'production mediawiki databases to collect ip info. For more information, see '
        'http://dev.mysql.com/doc/refman/5.1/en/option-files.html'
    )
    parser.add_argument(
        '--dest_sql_cnf',
        type=os.path.expanduser,
        default='~/.my.cnf.research',
        help='mysql ini-style option file which allows a user to write to the destination database'
        'for use with the write_*_sql output options.  For more information, see '
        'http://dev.mysql.com/doc/refman/5.1/en/option-files.html'
    )
    parser.add_argument(
        '--dest_db_name',
        default='staging',
        help='name of database in which to insert results'
    )
    parser.add_argument(
        '--active_editors_country',
        default=mysql_config.DEST_TABLE_NAMES['active_editors_country'],
        help='table in `dest_sql` db in which the active editor cohorts by country will be stored'
    )
    parser.add_argument(
        '--active_editors_world',
        default=mysql_config.DEST_TABLE_NAMES['active_editors_world'],
        help='table in `dest_sql` db in which the active editor cohorts for the entire world will be stored'
    )
    parser.add_argument(
        '--city_edit_fraction',
        default=mysql_config.DEST_TABLE_NAMES['city_edit_fraction'],
        help='table in `dest_sql` db in which the fraction of total country edits originating from'
        'the given city will be stored'
    )
    parser.add_argument(
        '--country_total_edit',
        default=mysql_config.DEST_TABLE_NAMES['country_total_edit'],
        help='table in `dest_sql` db in which the total number of edits from a given country will be stored'
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
        parser.error('no valid wikipedia projects recieved\n'
                     '       must either include the --wp flag or the --wpfiles flag\n')

    if not args.threads:
        setattr(args, 'threads', min(len(args.wp_projects), 30))
        logger.info('Running with %d threads', len(args.wp_projects))

    if args.quiet:
        logger.setLevel(logging.INFO)

    # create top-level dir
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    args.subdir = '%s_%s' % (
        datetime.date.strftime(args.start, '%Y%m%d'),
        datetime.date.strftime(args.end, '%Y%m%d'))

    # check for mysql login credentials
    if not os.path.exists(os.path.expanduser("~/.my.cnf")):
        logger.error("~/.my.cnf does not exist! MySql connection might fail")

    logger.info('args: %s', pprint.pformat(vars(args), indent=2))
    return vars(args)


def main():
    """Entry point for geo coding package
    """

    opts = parse_args()
    if opts['daily']:
        orig_start = copy.deepcopy(opts['start'])
        orig_end = copy.deepcopy(opts['end'])
        for day in [orig_start + datetime.timedelta(days=n) for n in range((orig_end - orig_start).days)]:
            logging.info('processing day: %s', day)
            opts['start'] = day - datetime.timedelta(days=30)
            opts['end'] = day
            # give each run its own dir
            opts['subdir'] = './%s_%s' % (
                datetime.date.strftime(opts['start'], '%Y%m%d'),
                datetime.date.strftime(opts['end'], '%Y%m%d'))

            if not os.path.exists(os.path.join(opts['output_dir'], opts['subdir'])):
                os.makedirs(os.path.join(opts['output_dir'], opts['subdir']))
            # log to file in subdir
            fh = logging.FileHandler(os.path.join(opts['output_dir'], opts['subdir'], 'log'))
            fh.setLevel(logging.DEBUG)
            logger.addHandler(fh)

            logger.info('running daily with options: %s', pprint.pformat(opts, indent=2))

            run_parallel(opts)
    else:
        if not os.path.exists(os.path.join(opts['output_dir'], opts['subdir'])):
            os.makedirs(os.path.join(opts['output_dir'], opts['subdir']))
        logger.addHandler(logging.FileHandler(os.path.join(opts['output_dir'], opts['subdir'], 'log')))
        run_parallel(opts)

if __name__ == '__main__':
    try:
        main()
    except:
        logger.error(traceback.format_exc())
        raise
