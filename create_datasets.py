"""
# Geodata

Export geo location data from the recent_changes table. The script is running multiple languages in parallel. For each language it does the following steps.

1. Export the recent_changes tables to avoid doing analytics on a production server
2. Exract/Transform/Load on exported data

"""

from multiprocessing import Pool
import os

# if only we could use a mysql driver....
# try:
#     import MySQLdb,MySQLdb.cursors
# except:
#     print "Warning: SQL module MySQLdb could not be imported."

import geo_coding as gc
import languages
import mysql_config

data_dir = './data'
output_dir = './output'


def export_data(lang,compressed=False):
	'''Dumps the needed entries from the recent changes table for language lang'''

	host_name = mysql_config.get_host_name(lang)
	db_name = mysql_config.get_db_name(lang)
	# user_name = mysql_config.user_name
	# pw = mysql_config.password

	
	# mysql query to export recent changes data
	export_query = "'SELECT rc_user_text, rc_ip, rc_new_len-rc_old_len AS len_change FROM %s.recentchanges rc WHERE rc.rc_namespace=0 AND rc.rc_user!=0 AND rc_bot=0'"%db_name


	if compressed:
		output_fn = os.path.join(data_dir,'%s_geo.tsv.gz'%lang)	
		# export_command = ['mysql', '-h', host_name,  '-u%s'%user_name,  '-p%s'%pw ,'-e', export_query, '|', 'gzip', '-c' ,'>', output_fn]
		export_command = ['mysql', '-h', host_name ,'-e', export_query, '|', 'gzip', '-c' ,'>', output_fn]

	else:
		output_fn = os.path.join(data_dir,'%s_geo.tsv'%lang)		
		# export_command = ['mysql', '-h', host_name,  '-u%s'%user_name,  '-p%s'%pw ,'-e', export_query, '>', output_fn]
		export_command = ['mysql', '-h', host_name ,'-e', export_query, '>', output_fn]



	# use problematic os.system instead of subprocess
	os.system(' '.join(export_command))	

	# if only we could use a mysql driver....
	# db = MySQLdb.connect(host=host_name,user=user_name,passwd=pw)
	# SScur = db.cursor(MySQLdb.cursors.SSCursor)
	# return SScur.execute(export_query)

	return output_fn



def create_dataset(lang):

	print 'CREATING DATASET FOR ',lang

	# export the data from mysql
	fn = export_data(lang)	

	# EXTRACT
	(editors,countries_cities) = gc.extract(fn)

	# TRANSFORM (only for editors)
	countries_editors = gc.transform(editors)

	# LOAD 
	gc.load(lang,countries_editors,countries_cities,output_dir)

	# delete the exported data
	# os.system('rm %s'%fn)

	print 'DONE : ',lang


if __name__ == '__main__':

	# check that data/output directories exist, create if not
	# todo move into an arg parser
	if not os.path.exists(data_dir):
		os.mkdir(data_dir)
	if not os.path.exists(output_dir):
		os.mkdir(output_dir)



	p = Pool(3)

	# languages = languages.languages
	languages =  ['ar']
	p.map(create_dataset, languages)

	print 'All languages done. Results are in %s folder'%(output_dir)

