#! /usr/bin/python

import argparse
import logging
import json, pprint
import datetime, dateutil.parser
from collections import defaultdict, OrderedDict, Container
import itertools
import operator
from operator import itemgetter
import re
import os
import MySQLdb as sql
#import sqlite3 as sql
import MySQLdb.cursors
import multiprocessing

import limnpy
#import gcat
import wikipandas
import pandas as pd

root_logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(levelname)s]\t[%(name)s]\t[%(funcName)s:%(lineno)d]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(logging.DEBUG)

logger = logging.getLogger(__name__)

# Parameters for interacting with Google Drive doc which holds Global South categories etc.
META_DATA_TITLE    = 'Global South and Region Classifications.bak'
META_DATA_SHEET    = 'data'
META_DATA_COUNTRY_FIELD = 'Country'
META_DATA_REGION_FIELD = 'Region'
META_DATA_GLOBAL_SOUTH_FIELD = 'Global South'

LIMN_GROUP = 'gp'

def make_limn_rows(rows, col_prim_key, count_key = 'count'):
    if not rows:
        return
    logger.debug('making limn rows from rows with keys:%s', rows[0].keys())
    logger.debug('col_prim_key: %s', col_prim_key)
    logger.debug('len(rows): %s', len(rows))
    rows = map(dict, rows) # need real dicts, not sqlite.Rows

    filtered = filter(lambda r : r['cohort'] in ['all', '5+', '100+'], rows)

    transformed = []
    # logger.debug('transforming rows to {\'date\' : end, \'%s (cohort)\' : count}', col_prim_key)
    for row in filtered:
        if col_prim_key in row:
            transformed.append({'date' : row['end'], '%s (%s)' % (row[col_prim_key], row['cohort']) : row[count_key]})
        else:
            logger.debug('row does not contain col_prim_key (%s): %s', col_prim_key, row)

    logger.debug('len(transformed): %s', len(transformed))
    limn_rows = []
    for date, date_rows in itertools.groupby(sorted(transformed, key=itemgetter('date')), key=itemgetter('date')):
        limn_row = {'date' : date}
        for date_row in date_rows:
            limn_row.update(date_row)
        limn_rows.append(limn_row)
    return limn_rows

def write_default_graphs(source, limn_id, limn_name, basedir):
    if source:
        source_id = source['id']

        cohorts = [('all', 'all', 'All'), ('5\+', 'active', 'Active'), ('100\+', 'very_active', 'Very Active')]
        for cohort_str, cohort_id, cohort_name in cohorts:
            cols = [name for name in source['columns']['labels'] if re.match('.*%s.*' % cohort_str, name)]
            source_cols = list(itertools.product([source_id], cols))
            limnpy.write_graph(limn_id + '_' + cohort_id, cohort_name + ' ' + limn_name, [source], source_cols, basedir=basedir)


def write_project_mysql(proj, cursor, basedir, country_graphs=False):
    logger.debug('writing project datasource for: %s', proj)
    limn_id = proj + '_all'
    limn_name = '%s Editors by Country' % proj.upper()

    if sql.paramstyle == 'qmark':
        query = """ SELECT * FROM erosen_geocode_active_editors_country WHERE project=? AND end = start + INTERVAL 30 day"""
        logger.debug('making query: %s', query)
    elif sql.paramstyle == 'format':
        query = """ SELECT * FROM erosen_geocode_active_editors_country WHERE project=%s AND end = start + INTERVAL 30 day"""
    cursor.execute(query, [proj])
    proj_rows = cursor.fetchall()

    logger.debug('len(proj_rows): %d', len(proj_rows))
    if not proj_rows and sql.paramstyle == 'format':
        logger.debug('GOT NUTHIN!: %s', query % proj)
        return
    limn_rows = make_limn_rows(proj_rows, 'country')
    source = limnpy.DataSource(limn_id, limn_name, limn_rows, limn_group=LIMN_GROUP)
    source.write(basedir=basedir)
    source.write_graph(basedir=basedir)

    # construct single column graphs
    if country_graphs:
        for country in source.data.columns[1:]:
            title = '%s Editors in %s' % (proj.upper(), country)
            graph_id = '%s_%s' % (proj, re.sub('\W+', ' ', country).strip().replace(' ', '_').lower())
            source.write_graph(metric_ids=[country], basedir=basedir, title=title, graph_id=graph_id)


def write_project_top_k_mysql(proj, cursor,  basedir, k=10):
    logger.debug('entering')
    limn_id = proj + '_top%d' % k
    limn_name = '%s Editors by Country (top %d)' % (proj.upper(), k)

    if sql.paramstyle == 'qmark':
        top_k_query = """SELECT country
                    FROM erosen_geocode_active_editors_country
                    WHERE project=? AND cohort='all' AND end = start+INTERVAL 30 day
                    GROUP BY country
                    ORDER BY SUM(count) DESC, end DESC, country
                    LIMIT ?"""
    elif sql.paramstyle == 'format':
        top_k_query = """SELECT country
                    FROM erosen_geocode_active_editors_country
                    WHERE project=%s AND cohort='all' AND end = start+INTERVAL 30 day
                    GROUP BY country
                    ORDER BY SUM(count) DESC, end DESC, country
                    LIMIT %s"""
        logger.debug('top k query: %s', top_k_query % (proj, k))
    cursor.execute(top_k_query, (proj, k)) # mysqldb first converts all args to str
    top_k = map(itemgetter('country'), cursor.fetchall())
    logger.debug('proj: %s, top_k countries: %s', proj, top_k)
    if not top_k:
        logger.warning('not country edits found for proj: %s', proj)
        return

    if sql.paramstyle == 'qmark':
        country_fmt = ', '.join([' ? ']*len(top_k))
        query = """ SELECT * FROM erosen_geocode_active_editors_country WHERE project=? AND country IN %s AND end = start + INTERVAL 30 day"""
        query = query % country_fmt
    elif sql.paramstyle == 'format':
        country_fmt = '(%s)' % ', '.join([' %s ']*len(top_k))
        logger.debug('country_fmt: %s', country_fmt)
        query = """ SELECT * FROM erosen_geocode_active_editors_country WHERE project=%s  AND end = start + INTERVAL 30 day AND country IN """
        query = query + country_fmt
        args = [proj]
        args.extend(top_k)
        print_query = query % tuple(args)
        logger.debug('top_k edit count query: %s', print_query)
    cursor.execute(query, [proj,] + top_k)
    proj_rows = cursor.fetchall()

    logger.debug('retrieved %d rows', len(proj_rows))
    limn_rows = make_limn_rows(proj_rows, 'country')
    source = limnpy.DataSource(limn_id, limn_name, limn_rows, limn_group=LIMN_GROUP)
    source.write(basedir=basedir)
    source.write_graph(basedir=basedir)


def write_overall_mysql(projects, cursor, basedir):
    logger.info('writing overall datasource')
    limn_id = 'overall_by_lang'
    limn_name = 'Overall Editors by Language'

    query = """ SELECT * FROM erosen_geocode_active_editors_world"""
    cursor.execute(query)
    overall_rows = cursor.fetchall()

    limn_rows = make_limn_rows(overall_rows, 'project')
    monthly_limn_rows = filter(lambda r: r['date'].day==1, limn_rows)
    #logger.debug('overall limn_rows: %s', pprint.pformat(limn_rows))
    source = limnpy.DataSource(limn_id, limn_name, limn_rows, limn_group=LIMN_GROUP)
    source.write(basedir=basedir)
    source.write_graph(basedir=basedir)

    monthly_source = limnpy.DataSource(limn_id+'_monthly', limn_name+' Monthly', monthly_limn_rows, limn_group=LIMN_GROUP)
    monthly_source.write(basedir=basedir)


def merge_rows(group_keys, rows, merge_key='count', merge_red_fn=operator.__add__, red_init=0):
    logger.debug('merging rows by grouping on: %s', group_keys)
    logger.debug('reducing field %s with fn: %s, init_val: %s', merge_key, merge_red_fn, red_init)
    group_vals = map(rows.distinct, group_keys)
    #logger.debug('group_vals: %s', pprint.pformat(dict(zip(group_keys, group_vals))))
    merged_rows = []
    for group_val in itertools.product(*group_vals):
        group_probe = dict(zip(group_keys, group_val))
        group_rows = rows.find(group_probe)
        merge_set = map(itemgetter(merge_key), group_rows)
        merged_val = reduce(merge_red_fn, merge_set, red_init)
        group_probe[merge_key] = merged_val
        merged_rows.append(group_probe)
    return merged_rows


def join(join_key, coll1, coll2):
    logger.debug('joining...')
    intersection = set(coll1.distinct(join_key)) & set(coll2.distinct(join_key))
    joined_rows = []
    for val in intersection:
        probe = {join_key : val}
        pairs = itertools.product(coll1.find(probe), coll2.find(probe))
        for row1, row2 in pairs:
            joined_rows.append(dict(row1.items() + row2.items()))
    logger.debug('done')
    return joined_rows


def write_group_mysql(group_key, country_data, cursor, basedir):
    logger.debug('writing group with group_key: %s', group_key)
    country_data = filter(lambda row: group_key in row, country_data)
    country_data = sorted(country_data, key=itemgetter(group_key))
    groups = itertools.groupby(country_data, key=itemgetter(group_key))
    groups = dict(map(lambda (key, rows) : (key, map(itemgetter(META_DATA_COUNTRY_FIELD), rows)), groups))
    #logger.debug(pprint.pformat(groups))
    all_rows = []
    for group_val, countries in groups.items():
        logger.debug('processing group_val: %s', group_val)
        if sql.paramstyle == 'qmark':
            group_query = """SELECT end, cohort, SUM(count)
                         FROM erosen_geocode_active_editors_country
                         WHERE country IN (%s)
                         AND end = start + INTERVAL 30 day
                         GROUP BY end, cohort"""
            countries_fmt = ', '.join([' ? ']*len(countries))
        elif sql.paramstyle == 'format':
            group_query = """SELECT end, cohort, SUM(count)
                         FROM erosen_geocode_active_editors_country
                         WHERE country IN (%s)
                         AND end = start + INTERVAL 30 day
                         GROUP BY end, cohort"""
            countries_fmt = ', '.join([' %s ']*len(countries))
        group_query_fmt = group_query % countries_fmt
        cursor.execute(group_query_fmt, tuple(countries))
        group_rows = cursor.fetchall()
        group_rows = map(dict, group_rows)
        for row in group_rows:
            row.update({group_key : group_val})
        all_rows.extend(group_rows)
    #logger.debug('groups_rows: %s', group_rows)

    limn_rows = make_limn_rows(all_rows, group_key, count_key='SUM(count)')
    limn_id = group_key.replace(' ', '_').lower()
    limn_name = group_key.title()
    logger.debug('limn_rows: %s', limn_rows)
    source = limnpy.DataSource(limn_id, limn_name, limn_rows, limn_group=LIMN_GROUP)
    source.write(basedir=basedir)
    source.write_graph(basedir=basedir)

def get_countries(project, cursor):
    query = """SELECT DISTINCT(country) FROM erosen_geocode_active_editors_country
               WHERE project=%s"""
    cursor.execute(query, (project,))
    countries = [row['country'] for row in cursor.fetchall()]
    return countries

def write_project_country_language(project, cursor, basedir):
    for country in get_countries(project, cursor):
        limn_id = '%s_%s' % (project, country.replace(' ', '_').replace('/', '-').lower())
        limn_name = '%s Editors in %s' % (project.upper(), country.title())
        query = """SELECT country, end, cohort, count FROM erosen_geocode_active_editors_country
                   WHERE project=%s AND country=%s"""
        cursor.execute(query, (project, country))
        country_rows = cursor.fetchall()

        limn_rows = make_limn_rows(country_rows, 'country')
        source = limnpy.DataSource(limn_id, limn_name, limn_rows, limn_group=LIMN_GROUP)
        source.write(basedir=basedir)
        source.write_graph(basedir=basedir)

def parse_args():

    parser = argparse.ArgumentParser(description='Format a collection of json files output by editor-geocoding and creates a single csv in digraph format.')
    parser.add_argument(
        '--geo_files',
        metavar='GEOCODING_FILE.json',
        nargs='+',
        help='any number of appropriately named json files')
    parser.add_argument(
        '-d','--basedir',
        default='/home/erosen/src/dashboard/geowiki/data',
        help='directory in which to find or create the datafiles and datasources directories for the *.csv and *.yaml files')
    parser.add_argument(
        '-b', '--basename',
        default='geo_editors',
        help='base file name for csv and yaml files.  for example: BASEDIR/datasources/BAS_FILENAME_en.yaml')
    parser.add_argument(
        '-k',
        type=int,
        default=10,
        help='the number of countries to include in the selected project datasource')
    parser.add_argument(
        '-p', '--parallel',
        action='store_true',
        default=True,
        help='use a multiprocessing pool to execute per-language analysis in parallel'
        )
    parser.add_argument(
        '--source_sql_cnf',
        type=os.path.expanduser,
        default='~/.my.cnf.research',
        help='mysql ini-style option to connect to a database containing the '
	'data to limnify. This configuration is usually the one you used as '
	'--dest_sql_cnf for process_data.py. '
	'(default: ~/.my.cnf.research)'
        )
    parser.add_argument(
        '--source_db_name',
        default='staging',
        help='name of database to get data from. This database is accessed '
	'through the credentials provided by --source-sql_cnf. '
	'(default: staging)'
        )

    args = parser.parse_args()
    logger.info(pprint.pformat(vars(args), indent=2))
    return args

def get_projects():
    f = open(os.path.join(os.path.split(__file__)[0], '..', 'geowiki', 'data', 'all_ids.tsv'))
    projects = []
    for line in f:
        projects.append(line.split('\t')[0].strip())
    return projects


def process_project_par((project, basedir)):
    try:
        logger.info('processing project: %s', project)
        db = sql.connect(read_default_file=args.source_sql_cnf, db=args.source_db_name)
        cursor = db.cursor(MySQLdb.cursors.DictCursor)

        # db = sql.connect('/home/erosen/src/editor-geocoding/geowiki.sqlite')

        write_project_mysql(project, cursor, args.basedir)
        write_project_top_k_mysql(project, cursor, args.basedir, k=args.k)
        #write_project_country_language(project, cursor, args.basedir)
    except:
        logger.exception('caught exception in process:')
        raise

def process_project(project, cursor, basedir):
    logger.info('processing project: %s (%d/%d)', project, i, len(projects))
    write_project_mysql(project, cursor, args.basedir)
    write_project_top_k_mysql(project, cursor, args.basedir, k=args.k)
    #write_project_country_language(project, cursor, args.basedir)

def plot_gs_editor_fraction(basedir):
    df = pd.read_csv(basedir + '/datafiles/global_south.csv', index_col='date', parse_dates=['date'])
    df['Global South Fraction (100+)'] = df['Global South (100+)'] / (df['Global South (100+)'] + df['Global North (100+)'] + df['Unkown (100+)']).apply(float)
    df['Global South Fraction (5+)']   = df['Global South (5+)'] / (df['Global South (5+)'] + df['Global North (5+)'] + df['Unkown (5+)']).apply(float)
    df['Global South Fraction (all)'] = df['Global South (all)'] / (df['Global South (all)'] + df['Global North (all)'] + df['Unkown (all)']).apply(float)
    df_frac = df[['Global South Fraction (100+)', 'Global South Fraction (5+)', 'Global South Fraction (all)']]

    ds_frac = limnpy.DataSource(limn_id='global_south_editor_fractions',
            limn_name='Global South Editor Fractions',
            limn_group=LIMN_GROUP,
            data = df_frac)
    ds_frac.write(basedir)
    g = ds_frac.get_graph(metric_ids=['Global South Fraction (5+)'],
            title='Global South Active Editor Fraction',
            graph_id='global_south_editor_fractions')
    g.write(basedir)

def drop_callout_widget(g):
    """Drop the callout widget from a graph

    Keyword arguments:
    graph -- limnpy Graph. Drop the change widget from this graph.
    """
    g.graph["root"]["children"] = [ widget \
        for widget in g.graph["root"]["children"] \
            if "nodeType" not in widget or widget["nodeType"] != "callout" ]

def plot_active_editor_totals(basedir):
    """Write out files for 'Active Editors Total' graph

    Keyword arguments:
    basedir -- string. Path to the data repository

    This function computes the total number of active editors and
    writes out the necessary datafile, datasource, and graph files to
    show them in Limn. Those files get written into the corresponding
    subdirectories of basedir.

    For the computation of the data, this function relies solely on
    the global_south.csv file.
    """
    df = pd.read_csv(basedir + '/datafiles/global_south.csv', index_col='date', parse_dates=['date'])
    df['Active Editors Total']   = (df['Global South (5+)']   + df['Global North (5+)']   + df['Unkown (5+)']  ).apply(float)
    df_total = df[['Active Editors Total']]

    ds_total = limnpy.DataSource(limn_id='active_editors_total',
            limn_name='Active Editors Total',
            limn_group=LIMN_GROUP,
            data = df_total)
    ds_total.write(basedir)
    g = ds_total.get_graph(metric_ids=['Active Editors Total'],
            title='Active Editors Total (Tentative)',
            graph_id='active_editors_total')
    g.graph['desc']="""This graph currently over-reports by counting each
active editor once for each distinct pair of project and country
associated to the IP addresses used by the editor.

Also, this graph currently only considers the following projects
(no wikidata, no wiktionaries, no wikiquotes, no wikibooks):

    """ + ("wiki, ".join(sorted(get_projects()))) + """wiki

"""
    drop_callout_widget(g)
    g.write(basedir)

if __name__ == '__main__':
    args = parse_args()

    db = MySQLdb.connect(read_default_file=args.source_sql_cnf, db=args.source_db_name, cursorclass=MySQLdb.cursors.DictCursor)
    # db = sql.connect('/home/erosen/src/editor-geocoding/geowiki.sqlite')
    # db.row_factory = sql.Row
    cursor = db.cursor()


    write_project_mysql('en', cursor, args.basedir, country_graphs=True)

    # # use metadata from Google Drive doc which lets us group by country
    #country_data = gcat.get_file(META_DATA_TITLE, sheet=META_DATA_SHEET, fmt='dict', usecache=False)
    country_data_df = wikipandas.get_table(title='List_of_Countries_by_Regional_Classification',
            site='meta.wikimedia.org',
            table_idx=0)
    country_data = [dict(series) for idx, series in country_data_df.iterrows()]
    # logger.debug('typ(country_data): %s', type(country_data))
    # logger.info('country_data[0].keys: %s', country_data[0].keys())

    write_group_mysql(META_DATA_GLOBAL_SOUTH_FIELD, country_data, cursor, args.basedir)
    write_group_mysql(META_DATA_REGION_FIELD, country_data, cursor, args.basedir)

    write_group_mysql(META_DATA_COUNTRY_FIELD, country_data, cursor, args.basedir)

    projects = get_projects()
    if not args.parallel or sql.threadsafety < 2:
        for i, project in enumerate(projects):
            logger.info('processing project: %s (%d/%d)', project, i, len(projects))
            process_project(project, cursor, args.basedir)
    else:
        pool = multiprocessing.Pool(20)
        pool.map_async(process_project_par, itertools.izip(projects, itertools.repeat(args.basedir))).get(99999)

    write_overall_mysql(projects, cursor, args.basedir)
    plot_gs_editor_fraction(args.basedir)
    plot_active_editor_totals(args.basedir)


