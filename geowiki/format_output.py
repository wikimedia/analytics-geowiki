import argparse
import logging as log
import json
import datetime, dateutil.parser
from collections import defaultdict, OrderedDict
from functools import partial
from nesting import Nest
from operator import itemgetter
import yaml, csv
import re
import os

root_logger = log.getLogger()
ch = log.StreamHandler()
formatter = log.Formatter('[%(levelname)s]\t[%(threadName)s]\t[%(funcName)s:%(lineno)d]\t%(message)s')
ch.setFormatter(formatter)
root_logger.addHandler(ch)
root_logger.setLevel(log.DEBUG)


def get_date(fname):
    dstr = os.path.split(fname)[1].split('_')[1]
    full_fmt = '%Y%m%d'
    monthly_fmt = '%Y%m'
    try:
        d = datetime.datetime.strptime(dstr, full_fmt)
    except ValueError:
        d = datetime.datetime.strptime(dstr, monthly_fmt)

def flatten(nest, path=[], keys=[]):
    #log.debug('entering with type(nest):%s,\tpath: %s' % (type(nest), path))
    # try to use as dict
    try:
        #log.debug('trying to use as dict')
        for k, v in nest.items():
            #log.debug('calling flatten(%s, %s)' % (k, v))
            for row in flatten(v, path + [k], keys):
                yield row
    except AttributeError:
        #log.debug('nest has not attribute \'items()\'')
        # try to use as list
        try:
            #log.debug('trying to use as list')
            for elem in nest:
                for row in flatten(elem, path, keys):
                    yield row
        except TypeError:
            #log.debug('nest object of type %s is not iterable' % (type(nest)))
            #log.debug('reached leaf of type: %s' % (type(nest)))
            # must be a leaf, finally yield
            #log.debug('yielding %s' % (path + [nest]))
            yield dict(zip(keys, path + [nest]))


def load_json_files(files):
    limn_fmt = '%Y/%m/%d'

    json_all = []
    for f in files:
        json_f = json.load(open(f, 'r'))
        json_f['end'] = dateutil.parser.parse(json_f['end']) 
        json_f['start'] = dateutil.parser.parse(json_f['start'])
        if (json_f['end'] - json_f['start']).days != 30:
            logging.info('skipping file: because it is not a 30 day period')
            continue
        json_f['end'] = json_f['end'].strftime(limn_fmt)
        json_f['start'] = json_f['start'].strftime(limn_fmt)
        json_all.append(json_f)
    return json_all


def get_rows(json_all):
    json_tree = defaultdict(dict)
    for json_f in json_all:
        json_tree[json_f['end']][json_f['project']] = json_f['countries']
    # log.debug('f: %s' % (json.dumps(json_all, indent=2)))
    # expand tree structure of dictionaries into list of dicts with named fields
    rows = list(flatten(json_tree, [], ['date', 'project', 'country', 'cohort', 'count']))
    by_date = Nest().key(itemgetter('date')).map(rows)
    # everything is by date, so everyone wants things sorted
    by_date = OrderedDict(sorted(by_date.items()))
    return by_date
    


def write_yaml(_id, name, fields, csv_name, rows, args):

    meta = {}
    meta['id'] = _id
    meta['name'] = name
    meta['shortName'] = meta['name']
    meta['format'] = 'csv'
    meta['url'] = '/data/datafiles/' + csv_name

    timespan = {}
    timespan['start'] = sorted(rows.keys())[0]
    timespan['end'] = sorted(rows.keys())[-1]
    #timespan['step'] = '1mo'
    timespan['step'] = '1d'
    meta['timespan'] = timespan

    columns = {}
    columns['types'] = ['int' for key in fields]
    columns['types'][0] = 'date' # first row is always a day
    columns['labels'] = fields
    meta['columns'] = columns

    meta['chart'] = {'chartType' : 'dygraphs'}

    yaml_name = args.basename + '_' + _id + '.yaml'
    yaml_path = os.path.join(args.datasource_dir, yaml_name)
    fyaml = open(yaml_path, 'w')
    fyaml.write(yaml.safe_dump(meta, default_flow_style=False))
    fyaml.close()
    return yaml_path


def top_k_countries(rows, k, filter_fn):
    country_totals = defaultdict(int)
    for date, row_batch in rows.items():
        filtered_batch = filter(filter_fn, row_batch)
        for row in filtered_batch:
            country_totals[row['country']] += row['count']
    #log.debug(sorted(map(list,map(reversed,country_totals.items())), reverse=True))
    keep_countries = zip(*sorted(map(list,map(reversed,country_totals.items())), reverse=True))[1][:k]
    #log.debug('keep_countries: %s', keep_countries)
    top_k_rows = OrderedDict()
    for date, row_batch in rows.items():
        filtered_batch = filter(lambda row: row['country'] in keep_countries, row_batch)
        top_k_rows[date] = filtered_batch
    return top_k_rows
    

def write_project_datasource(proj, rows, args, k=None):
    log.debug('writing project datasource for: %s, k=%s', proj, k)
    _id = proj + '_all'
    name = '%s Editors by Country' % proj.upper()

    if k:
        # only write top k countries
        _id = proj + '_top%d' % k
        name = '%s Editors by Country (top %d)' % (proj.upper(), k)
        rows = top_k_countries(rows, k, lambda row: row['project']==proj and row['cohort']=='all')

    csv_name = args.basename + '_' + _id + '.csv'
    csv_path = os.path.join(args.datafile_dir, csv_name)
    csv_file = open(csv_path, 'w')

    # remove rows that don't interest us and then grab the row id (country-cohort) and count
    csv_rows = []
    for date, row_batch in rows.items():
        filtered_batch = filter(lambda row : row['project'] == proj, row_batch)
        csv_row = {'date' : date}
        for row in filtered_batch:
            csv_row['%s (%s)' % (row['country'], row['cohort'])] = row['count']
        csv_rows.append(csv_row)

    # normalize fields
    all_fields = sorted(reduce(set.__ior__, map(set,map(dict.keys, csv_rows)), set()))
    all_fields.remove('date')
    all_fields.insert(0,'date')

    writer = csv.DictWriter(csv_file, all_fields, restval='', extrasaction='ignore')
    writer.writeheader()
    for csv_row in csv_rows:
        writer.writerow(csv_row)
    csv_file.close() 

    #def write_yaml(_id, name, fields, csv_name, rows, args):
    return write_yaml(_id, name, all_fields, csv_name, rows, args)



def write_overall_datasource(projects, json_all, args):
    log.info('writing overall datasource')
    _id = 'overall'
    name = 'Overall Editors by Language'

    # build rows
    keys = ['end', 'project', 'world']
    json_tree = defaultdict(lambda : defaultdict(int))
    for json_f in json_all:
        json_tree[json_f['end']][json_f['project']] = json_f['world']

    # expand cohorts
    rows = list(flatten(json_tree, [], ['date', 'project', 'cohort', 'count']))

    # group by date
    by_date = Nest().key(itemgetter('date')).map(rows)
    by_date = OrderedDict(sorted(by_date.items()))

    csv_name = args.basename + '_' + _id + '.csv'
    csv_path = os.path.join(args.datafile_dir, csv_name)
    csv_file = open(csv_path, 'w')

    # remove rows that don't interest us and then grab the row id (country-cohort) and count
    csv_rows = []
    for date, row_batch in by_date.items():
        # TODO: need to be extracting the top level field 'world' (note the lowercase)
        csv_row = {'date' : date}
        for row in row_batch:
            csv_row['%s (%s)' % (row['project'], row['cohort'])] = row['count']
        csv_rows.append(csv_row)

    # normalize fields
    all_fields = sorted(reduce(set.__ior__, map(set,map(dict.keys, csv_rows)), set()))
    all_fields.remove('date')
    all_fields.insert(0,'date')

    writer = csv.DictWriter(csv_file, all_fields, restval='', extrasaction='ignore')
    writer.writeheader()
    for csv_row in csv_rows:
        writer.writerow(csv_row)
    csv_file.close() 

    #def write_yaml(_id, name, fields, csv_name, rows, args):
    return write_yaml(_id, name, all_fields, csv_name, by_date, args)
    

def write_summary_graphs(json_all, args):
    for proj, rows in json_all.items():
        graph = {}
        graph['options'] = {
            "strokeWidth": 4,
            "pointSize": 4,
            "stackedGraph": true,
            "digitsAfterDecimal": 0,
            "drawPoints": true,
            "axisLabelFontSize": 12,
            "xlabel": "Date",
            "ylabel": "# Active Editors (>5 Edits)"
            }
        graph["name"] = "Arabic WP Active Editors by Country (stacked graph)",
        graph["notes"] = ""
        graph["callout"] = {
            "enabled": true,
            "metric_idx": 0,
            "label": ""
            }
        graph["slug"] = "ar_wp"
        graph["width"] = "auto"
        graph["parents"] = ["root"]
        graph["result"] = "ok"
        graph["id"] = "ar_wp"
        graph["chartType"] = "dygraphs"
        graph["height"] = 320
        metrics = []
        for i,  in enumerate(rows):
            if i >= k:
                break
            metric = {}
            metric["index"] = 1,
            metric["scale"] = 1,
            metric["timespan"] = {
            "start": null,
            "step": null,
            "end": null
            },
            metric["color"] = "#d53e4f",
            metric["format_axis"] = null,
            metric["label"] = "Algeria",
            metric["disabled"] = false,
            metric["visible"] = true,
            metric["format_value"] = null,
            metric["transforms"] = [],
            metric["source_id"] = "active_editors_ar",
            metric["chartType"] = null,
            metric["type"] = "int",
            metric["source_col"] = 5
            metrics.append(metric)
        data = {}
        data["metrics"] = metrics
        graph["data"] = data




def parse_args():

    parser = argparse.ArgumentParser(description='Format a collection of json files output by editor-geocoding and creates a single csv in digraph format.')
    parser.add_argument(
        'geo_files', 
        metavar='GEOCODING_FILE.json', 
        nargs='+',
        help='any number of appropriately named json files')
    parser.add_argument(
        '-s','--datasource_dir',
        default='./datasources',
        type=os.path.expanduser,
        nargs='?',
        help='directory in which to place *.csv files for limn')
    parser.add_argument(
        '-f', '--datafile_dir',
        default='./datafiles',
        type=os.path.expanduser,
        nargs='?', 
        help='directory in which to place the *.yaml files for limn')
    parser.add_argument(
        '-g', '--graphs_dir',
        default='./graphs', 
        type=os.path.expanduser,
        nargs='?',
        help='directory in which to place the *.json which represent graph metadata')
    parser.add_argument(
        '-b', '--basename',
        default='geo_editors',
        help='base file name for csv and yaml files.  for example: DATASOURCE_DIR/BAS_FILENAME_en.yaml')
    parser.add_argument(
        '-k', 
        type=int, 
        default=10, 
        help='the number of countries to include in the selected project datasource')

    args = parser.parse_args()

    for name in [args.datafile_dir, args.datasource_dir, args.graphs_dir]:
        if not os.path.exists(name):
            os.makedirs(name)

    log.info(json.dumps(vars(args), indent=2))
    return args

def main():
    args = parse_args()
    json_all = load_json_files(args.geo_files)
    projects = list(set(map(itemgetter('project'), json_all)))
    rows = get_rows(json_all)
    for project in projects:
        write_project_datasource(project, rows, args)
        write_project_datasource(project, rows, args, k = args.k)
    write_overall_datasource(projects, json_all, args)


if __name__ == '__main__':
    main()
