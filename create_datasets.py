"""
# Geodata

Export geo location data from the recent_changes table. The script is running multiple languages in parallel. For each language it does the following steps.

1. Export the recent_changes tables to avoid doing analytics on a production server
2. Exract/Transform/Load on exported data

"""

import os
from multiprocessing import Pool
import logging
logging.basicConfig(level=logging.INFO)


import geo_coding as gc
import languages
import mysql_config

data_dir = './data'
output_dir = './output'


def mysql_resultset(wp_pr):

	
	# query = mysql_config.construct_rc_query(db_name)	
	query = mysql_config.construct_cu_query(wp_pr,'201204')
	logging.info("SQL query for %s:\n\t%s"%(wp_pr,query))

	cur = mysql_config.get_cursor(wp_pr,server_side=True)
	cur.execute(query)

	return cur


def retrieve_bot_list(wp_pr):
	'''Returns a set of all known bots for `wp_pr`.
	'''	
	bot_fn = '%s_bot.tsv'%wp_pr

	query = mysql_config.construct_bot_query(wp_pr)

	# host_name = mysql_config.get_host_name(wp_pr)
	# bot_command = 'mysql -h %s -e "%s" > %s; sed -i "1d" %s'%(host_name,query,bot_fn,bot_fn)

	cur = mysql_config.get_cursor(wp_pr,server_side=False)
	cur.execute(query)

	return set(cur)



def dump_data_iterator(wp_pr,compressed=False):
	'''Dumps the needed entries from the recent changes table for project `wp_pr`.

	:returns: Iterable open file object 
	'''

	host_name = mysql_config.get_host_name(wp_pr)
	db_name = mysql_config.get_db_name(wp_pr)
	# user_name = mysql_config.user_name
	# pw = mysql_config.password

	
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



def create_dataset(wp_pr):

	logging.info('CREATING DATASET FOR %s'%wp_pr)

	# EXTRACT

	### export the data from mysql by dumping into a temp file
	# source = dump_data_iterator(wp_pr,compressed=True)		
	# (editors,countries_cities) = gc.extract(source=source,filter_id=(),sep='\t')
	
	# OR

	### use a server-side cursor to iterate the result set
	source = mysql_resultset(wp_pr)
	bots = retrieve_bot_list(wp_pr)
	(editors,cities) = gc.extract(source=source,filter_id=bots)

	# TRANSFORM (only for editors)
	countries_editors,countries_cities = gc.transform(editors,cities)

	# LOAD 
	gc.load(wp_pr,countries_editors,countries_cities,output_dir)

	# delete the exported data
	# os.system('rm %s'%fn)

	logging.info('DONE : %s'%wp_pr)


if __name__ == '__main__':

	# check that data/output directories exist, create if not
	# todo move into an arg parser
	# if not os.path.exists(data_dir):
	# 	os.mkdir(data_dir)
	if not os.path.exists(output_dir):
		os.mkdir(output_dir)

	p = Pool(4)

	# languages = languages.languages
	wp_projects =  ['ar','pt','hi','en']
	p.map(create_dataset, wp_projects)
	
	# test a project for debugging
	# create_dataset('ar')	

	logging.info('All projects done. Results are in %s folder'%(output_dir))

