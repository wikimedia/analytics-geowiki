"""Configuration file for the MediaWiki MySql databases.

The method `get_host_name(wp_pr)` returns a host name for the Wikipedia project passed as parameter.

The login info has to to be configured by creating the file `~/.my.cnf` with the following conent:


"""
import os
import logging

from datetime import datetime
import calendar


# check for login credentials
if not os.path.exists(os.path.expanduser("~/.my.cnf")):
	logging.info("~/.my.cnf does not exist! MySql connection might fail.")


# user_name = 'wikiadmin'
# password = 'test'

# mysql query for the recent changes data
recentchanges_query = "SELECT rc.rc_user_text, rc.rc_ip FROM %s.recentchanges rc WHERE rc.rc_namespace=0 AND rc.rc_user!=0 AND rc.rc_bot=0"
checkuser_query = "SELECT cuc.cuc_user_text, cuc.cuc_ip FROM %s.cu_changes cuc WHERE cuc.cuc_namespace=0 AND cuc.cuc_user!=0 AND cuc.cuc_timestamp>%s AND cuc.cuc_timestamp<%s"

def construct_rc_query(db_name):
	'''Constructs a query for the recentchanges table for a given month.
	'''
	return recentchanges_query%db_name

def construct_cu_query(db_name,ts=None):
	'''Constructs a query for the checkuser table for a given month.

	:arg ts: str, timestamp '201205'. If None, last 30 days will be used. 
	'''	
	def wiki_timestamp(dt):
		return datetime.strftime(dt,'%Y%m%d%H%M%S')

	if ts:
		y = int(ts[:4])
		m = int(ts[4:])
		start = datetime(y, m, 1)
		end = datetime(y, m, calendar.monthrange(y,m)[1], 23, 59, 59)
	else:
		from datetime import timedelta
		thirty = timedelta(days=30)
		end = datetime.now()
		start = end-thirty

	return checkuser_query%(db_name,wiki_timestamp(start),wiki_timestamp(end))



# wikimedia cluster information extracted from http://noc.wikimedia.org/conf/highlight.php?file=db.php
# NOTE: The default mapping is 's3'
cluster_mapping = {'enwiki':'s1','bgwiki':'s2','bgwiktionary':'s2','cswiki':'s2','enwikiquote':'s2','enwiktionary':'s2','eowiki':'s2','fiwiki':'s2','idwiki':'s2','itwiki':'s2','nlwiki':'s2','nowiki':'s2','plwiki':'s2','ptwiki':'s2','svwiki':'s2','thwiki':'s2','trwiki':'s2','zhwiki':'s2','commonswiki':'s4','dewiki':'s5','frwiki':'s6','jawiki':'s6','ruwiki':'s6','eswiki':'s7','huwiki':'s7','hewiki':'s7','ukwiki':'s7','frwiktionary':'s7','metawiki':'s7','arwiki':'s7','centralauth':'s7','cawiki':'s7','viwiki':'s7','fawiki':'s7','rowiki':'s7','kowiki':'s7'}

db_mapping = {'s2':'db1018', 's1':'db1033', 's7':'db1024', 's6':'db1040', 's5':'db1021', 's4':'db1004','s3':'db1035'}


def get_db_name(wp_pr):
	'''Returns the name of the database'''
	return '%swiki'%wp_pr


def get_host_name(wp_pr):
	'''Returns the host name for the wiki project wp_pr'''
	wiki = get_db_name(wp_pr)
	cluster = cluster_mapping[wiki] if wiki in cluster_mapping else 's3'
	host = db_mapping[cluster]
	return '%s.eqiad.wmnet'%host