"""Configuration file for the MediaWiki MySql databases.

The login info has to to be configured by creating the file `~/.my.cnf` with the following conent:

    [client]
    user = USERNAME
    password = PASSWORD


"""
import logging
import os

from datetime import datetime
from collections import OrderedDict
import codecs
import json

try:
    import MySQLdb
    import MySQLdb.cursors
except:
    pass

logger = logging.getLogger(__name__)
#logger = logging.getLogger()
print 'msyql_config __name__: %s' % __name__


# export all known bots for a wiki
bot_query = "SELECT ug.ug_user FROM %s.user_groups ug WHERE ug.ug_group = 'bot'"


def construct_bot_query(wp_pr):
    '''Returns a set of all known bots for the `db_name` wp project database
    '''
    print 'mysql_config logger: %s' % logger
    return bot_query % (get_db_name(wp_pr))

# mysql query for the recent changes data
recentchanges_query = "SELECT rc.rc_user, rc.rc_ip FROM %s.recentchanges rc WHERE rc.rc_namespace=0 AND rc.rc_user!=0 AND rc.rc_bot=0"


def construct_rc_query(wp_pr):
    '''Constructs a query for the recentchanges table for a given month.
    '''
    return recentchanges_query % get_db_name(wp_pr)

# mysql query for the check user data
#checkuser_query = "SELECT cuc.cuc_user, cuc.cuc_ip FROM %s.cu_changes cuc WHERE cuc.cuc_namespace=0 AND cuc.cuc_user!=0 AND cuc.cuc_timestamp>=%s AND cuc.cuc_timestamp<%s"
checkuser_query = "SELECT cuc.cuc_user, cuc.cuc_ip FROM %s.cu_changes cuc WHERE cuc.cuc_namespace=0 AND cuc.cuc_user!=0 AND cuc.cuc_timestamp>'%s' AND cuc.cuc_timestamp<'%s'"


def construct_cu_query(wp_pr, start, end):
    '''Constructs a query for the checkuser table for a given month. The timestamp `ts` can be either:
        * `201205`, data for the month of May 2012
        * `20120525`, the last 30 days from the day passed.
        * None, last 30 days from now()

    Note:
        The checkuser `cu_changes` table contains data for the last three month only!

    :arg ts: str, timestamp '201205'. If None, last 30 days will be used.
    '''
    def wiki_timestamp(dt):
        return datetime.strftime(dt, '%Y%m%d%H%M%S')

    # if ts:
    #   if len(ts)==6:
    #       y = int(ts[:4])
    #       m = int(ts[4:])
    #       start = datetime(y, m, 1)
    #       end = datetime(y, m, calendar.monthrange(y,m)[1], 23, 59, 59)
    #   if len(ts)==8:
    #       y = int(ts[:4])
    #       m = int(ts[4:6])
    #       d = int(ts[6:8])
    #       thirty = timedelta(days=30)
    #       end = datetime(y, m, d, 23, 59, 59)
    #       start = end-thirty
    # else:
    #   thirty = timedelta(days=30)
    #   end = datetime.now()
    #   start = end-thirty

    return checkuser_query % (
        get_db_name(wp_pr),
        wiki_timestamp(start),
        wiki_timestamp(end)
    )


# wikimedia cluster information extracted from:
# http://noc.wikimedia.org/conf/highlight.php?file=db.php
# NOTE: The default mapping is 's3'
cluster_mapping = {
    'enwiki': 's1',
    'bgwiki': 's2',
    'bgwiktionary': 's2',
    'cswiki': 's2',
    'enwikiquote': 's2',
    'enwiktionary': 's2',
    'eowiki': 's2',
    'fiwiki': 's2',
    'idwiki': 's2',
    'itwiki': 's2',
    'nlwiki': 's2',
    'nowiki': 's2',
    'plwiki': 's2',
    'ptwiki': 's2',
    'svwiki': 's2',
    'thwiki': 's2',
    'trwiki': 's2',
    'zhwiki': 's2',
    'commonswiki': 's4',
    'dewiki': 's5',
    'frwiki': 's6',
    'jawiki': 's6',
    'ruwiki': 's6',
    'eswiki': 's7',
    'huwiki': 's7',
    'hewiki': 's7',
    'ukwiki': 's7',
    'frwiktionary': 's7',
    'metawiki': 's7',
    'arwiki': 's7',
    'centralauth': 's7',
    'cawiki': 's7',
    'viwiki': 's7',
    'fawiki': 's7',
    'rowiki': 's7',
    'kowiki': 's7',
}

# all databases can be found on analytics-store
db_mapping = {
    's1': 'analytics-store.eqiad.wmnet',
    's2': 'analytics-store.eqiad.wmnet',
    's3': 'analytics-store.eqiad.wmnet',
    's4': 'analytics-store.eqiad.wmnet',
    's5': 'analytics-store.eqiad.wmnet',
    's6': 'analytics-store.eqiad.wmnet',
    's7': 'analytics-store.eqiad.wmnet',
}


def get_db_name(wp_pr):
    '''Returns the name of the database'''
    return '%swiki' % wp_pr


def get_host_name(wp_pr):
    '''Returns the host name for the wiki project wp_pr'''
    wiki = get_db_name(wp_pr)
    cluster = cluster_mapping.get(wiki, 's3')
    return db_mapping[cluster]


def get_analytics_db_connection(wp_pr, opts):
    '''Returns a MySql connection to `wp_pr`, e.g. `en`'''

    host_name = get_host_name(wp_pr)
    #db_name = get_db_name(wp_pr)

    db = MySQLdb.connect(
        host=host_name,
        read_default_file=opts['source_sql_cnf'])
    #logging.info('Connected to [db:%s,host:%s]'%(db_name,host_name))
    return db


def get_analytics_cursor(wp_pr, opts, server_side=False):
    '''Returns a server-side cursor

    :arg wp_pr: str, Wikipedia project (e.g. `en`)
    :arg server_side: bool, if True returns a server-side cursor. Default is False
    '''
    db = get_analytics_db_connection(wp_pr, opts)
    cur = db.cursor(MySQLdb.cursors.SSCursor) if server_side else db.cursor(MySQLdb.cursors.Cursor)

    return cur


### output mysql stuff

DEST_TABLE_NAMES = {
    'active_editors_country': 'erosen_geocode_active_editors_country',
    'active_editors_world': 'erosen_geocode_active_editors_world',
    'city_edit_fraction': 'erosen_geocode_city_edit_fraction',
    'country_total_edit': 'erosen_geocode_country_edits'
}

DEST_TABLES = {}
DEST_TABLES['active_editors_country'] = OrderedDict([
    ('project', 'VARCHAR(255)'),
    ('country', 'VARCHAR(255)'),
    ('cohort', 'VARCHAR(255)'),
    ('start', 'DATE'),
    ('end', 'DATE'),
    ('count', 'INT'),
    ('ts', 'TIMESTAMP')])

DEST_TABLES['active_editors_world'] = OrderedDict([
    ('project', 'VARCHAR(255)'),
    ('cohort', 'VARCHAR(255)'),
    ('start', 'DATE'),
    ('end', 'DATE'),
    ('count', 'INT'),
    ('ts', 'TIMESTAMP')])

DEST_TABLES['city_edit_fraction'] = OrderedDict([
    ('project', 'VARCHAR(255)'),
    ('country', 'VARCHAR(255)'),
    ('city', 'VARCHAR(255)'),
    ('start', 'DATE'),
    ('end', 'DATE'),
    ('fraction', 'FLOAT'),
    ('ts', 'TIMESTAMP')])

DEST_TABLES['country_total_edit'] = OrderedDict([
    ('project', 'VARCHAR(255)'),
    ('country', 'VARCHAR(255)'),
    ('start', 'DATE'),
    ('end', 'DATE'),
    ('edits', 'INT'),
    ('ts', 'TIMESTAMP')])


def create_dest_tables(cursor, opts):

    for table_id, field_types in DEST_TABLES.items():
        table_name = opts[table_id]
        field_str = ',\n'.join(map(' '.join, field_types.items()))
        command = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (table_name, field_str)
        cursor.execute(command)
    cursor.analytics_db.commit()


def get_dest_cursor(opts):
    #logging.debug('connecting to destination mysql instance with credentials from: %s', opts['dest_sql_cnf'])
    db = MySQLdb.connect(read_default_file=opts['dest_sql_cnf'], db=opts['dest_db_name'])
    cur = db.cursor(MySQLdb.cursors.Cursor)
    #create_dest_tables(cur, opts)
    cur.analytics_db = db
    return cur


def write_country_active_editors_mysql(active_editors_by_country, opts, cursor):
    table_id = 'active_editors_country'
    table = opts[table_id]
    fields = DEST_TABLES[table_id].keys()
    fields.remove('ts')
    dict_fmt = ', '.join(map(lambda f: '%%(%s)s' % f, fields))
    query_fmt = """REPLACE INTO %s (%s) VALUES (%s);""" % (table, ','.join(fields), dict_fmt)
    #logging.debug(query_fmt)
    cursor.executemany(query_fmt, active_editors_by_country)
    cursor.analytics_db.commit()


def write_world_active_editors_mysql(world_active_editors, opts, cursor):
    table_id = 'active_editors_world'
    table = opts[table_id]
    fields = DEST_TABLES[table_id].keys()
    fields.remove('ts')
    dict_fmt = ', '.join(map(lambda f: '%%(%s)s' % f, fields))
    query_fmt = """REPLACE INTO %s (%s) VALUES (%s);""" % (table, ','.join(fields), dict_fmt)
    cursor.executemany(query_fmt, world_active_editors)
    cursor.analytics_db.commit()


def write_city_edit_fraction_mysql(city_edit_fractions, opts, cursor):
    table_id = 'city_edit_fraction'
    table = opts[table_id]
    fields = DEST_TABLES[table_id].keys()
    fields.remove('ts')
    dict_fmt = ', '.join(map(lambda f: '%%(%s)s' % f, fields))
    query_fmt = """REPLACE INTO %s (%s) VALUES (%s);""" % (table, ','.join(fields), dict_fmt)
    cursor.executemany(query_fmt, city_edit_fractions)
    cursor.analytics_db.commit()


def write_country_total_edits_mysql(country_totals, opts, cursor):
    table_id = 'country_total_edit'
    table = opts[table_id]
    fields = DEST_TABLES[table_id].keys()
    fields.remove('ts')
    dict_fmt = ', '.join(map(lambda f: '%%(%s)s' % f, fields))
    query_fmt = """REPLACE INTO %s (%s) VALUES (%s);""" % (table, ','.join(fields), dict_fmt)
    cursor.executemany(query_fmt, country_totals)
    cursor.analytics_db.commit()


def get_filepath(_type, project, opts):
    dt_fmt = '%Y%m%d'
    fn = '%s.%s' % (
        '_'.join(
            [opts['basename'],
             project,
             _type,
             opts['start'].strftime(dt_fmt),
             opts['end'].strftime(dt_fmt)]),
        'json')
    return os.path.join(opts['output_dir'], opts['subdir'], fn)


def dump_json(project, _type, rows, opts):
    fp = get_filepath(_type, project, opts)
    f = codecs.open(fp, encoding='utf-8', mode='w')
    json.dump(rows, f, ensure_ascii=False)
