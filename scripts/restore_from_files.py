#!/usr/bin/python

import argparse
import MySQLdb
import json
import pprint
import os
import logging
import re
import sqlite3

from geowiki.mysql_config import DEST_TABLES, DEST_TABLE_NAMES

root_logger = logging.getLogger()
ch = logging.StreamHandler()
formatter = logging.Formatter('[%(name)s]\t[%(levelname)s]\t[%(processName)s]\t[%(filename)s:%(lineno)d]\t[%(funcName)s]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(logging.DEBUG)
#root_logger.setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


class ParseDictAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        d = json.loads(values)
        setattr(namespace, self.dest, d)


def parse_args():
    parser = argparse.ArgumentParser('Utility for restoring the geocoding mysql tables from json files',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--sqlite',
        action='store_true',
        default=False,
        help='use an sqlite database with filename --dest_db_name'
    )
    parser.add_argument(
        '--sqlite_db_file',
        default='geowiki.sqlite',
        help='filename to use for sqlite3 database if the --sqlite flag is used'
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
        '--table_names',
        default=DEST_TABLE_NAMES,
        action=ParseDictAction,
        help='{table id : table names} pairs in `dest_sql` db to use for storing each type of data'
    )

    default_patterns = {
        'active_editors_country': 'geowiki_\w+_country_active_editors_\d{8}_\d{8}.json',
        'active_editors_world': 'geowiki_\w+_world_active_editors_\d{8}_\d{8}.json',
        'city_edit_fraction': 'geowiki_\w+_city_fractions_\d{8}_\d{8}.json',
        'country_total_edit': 'geowiki_\w+_country_total_edits_\d{8}_\d{8}.json',
    }

    parser.add_argument(
        '--patterns',
        default=default_patterns,
        action=ParseDictAction,
        help='regex pattern used to (recursively) find the relevant files for each table in basedir'
    )

    parser.add_argument(
        '--tables',
        default=DEST_TABLES.keys(),
        choices=DEST_TABLES.keys(),
        nargs='+',
        help='which tables to restore'
    )

    parser.add_argument(
        '--basedir',
        default='./output',
        help='drectory in which to search for files matching the patterns which will then be used to restore the corresponding mysql table'
    )

    args = parser.parse_args()
    opts = vars(args)
    logger.info('opts: %s', pprint.pformat(opts))
    return opts


def get_cursor(opts):
    if not opts['sqlite']:
        db = MySQLdb.connect(read_default_file=opts['dest_sql_cnf'], db=opts['dest_db_name'])
    else:
        db = sqlite3.connect(opts['sqlite_db_file'])
    return db.cursor()


def restore_table(table_id, opts, cursor):
    table_name = opts['table_names'][table_id]
    pattern = opts['patterns'][table_id]
    logging.info('restoring table_id: %s to table_name: %s, from files in %s with pattern: %s', table_id, table_name, opts['basedir'], pattern)

    rows = []
    for (dirpath, dirnames, filenames) in os.walk(opts['basedir']):
        for fn in filenames:
            #logging.debug('re.match(\'%s\', \'%s\')', pattern, fn)
            if re.match(pattern, fn):
                # logging.debug('found matching file: %s', fn)
                fp = os.path.join(dirpath, fn)
                contents = open(fp, 'r').read()
                if contents:
                    rows.extend(json.loads(contents))
                else:
                    logging.warning('found empty file: %s', fn)

    fields = DEST_TABLES[table_id].keys()
    fields.remove('ts')
    if opts['sqlite']:
        field_str = ',\n'.join(map(' '.join, DEST_TABLES[table_id].items()))
        command = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (table_name, field_str)
        logging.debug('creating sqlite table with command:\n%s', command)
        cursor.execute(command)
        cursor.connection.commit()
        dict_fmt = ', '.join(map(lambda f: ':%s' % f, fields))
    else:
        dict_fmt = ', '.join(map(lambda f: '%%(%s)s' % f, fields))
    query_fmt = """REPLACE INTO %s (%s) VALUES (%s);""" % (table_name, ','.join(fields), dict_fmt)
    if rows:
        logging.debug('loaded %d rows', len(rows))
        logging.debug('type(rows[0][\'start\']): %s', type(rows[0]['start']))
        logging.debug('query: %s', query_fmt % rows[0])
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            cursor.executemany(query_fmt, rows[i:i + batch_size])
            cursor.connection.commit()
            logging.debug('commited %d rows', i + batch_size)


def main():
    opts = parse_args()
    cursor = get_cursor(opts)
    for table_id in opts['tables']:
        logger.debug('restoring table_id: %s, table_name:%s', table_id, opts['table_names'][table_id])
        restore_table(table_id, opts, cursor)

if __name__ == '__main__':
    main()
