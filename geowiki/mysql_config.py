"""Configuration file for the MediaWiki MySql databases.

The login info has to to be configured by creating the file `~/.my.cnf` with the following conent:

	[client]
	user = USERNAME
	password = PASSWORD


"""
import os,logging

from datetime import datetime,timedelta
import calendar

try:
	import MySQLdb,MySQLdb.cursors
except:
	pass

logger = logging.getLogger(__name__)

# export all known bots for a wiki
bot_query = "SELECT ug.ug_user FROM %s.user_groups ug WHERE ug.ug_group = 'bot'"
def construct_bot_query(wp_pr):
	'''Returns a set of all known bots for the `db_name` wp project database
	'''
	return bot_query%(get_db_name(wp_pr))

# mysql query for the recent changes data
recentchanges_query = "SELECT rc.rc_user, rc.rc_ip FROM %s.recentchanges rc WHERE rc.rc_namespace=0 AND rc.rc_user!=0 AND rc.rc_bot=0"
def construct_rc_query(wp_pr):
	'''Constructs a query for the recentchanges table for a given month.
	'''
	return recentchanges_query%get_db_name(wp_pr)

# mysql query for the check user data
checkuser_query = "SELECT cuc.cuc_user, cuc.cuc_ip FROM %s.cu_changes cuc WHERE cuc.cuc_namespace=0 AND cuc.cuc_user!=0 AND cuc.cuc_timestamp>%s AND cuc.cuc_timestamp<%s"
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
		return datetime.strftime(dt,'%Y%m%d%H%M%S')

	# if ts:
	# 	if len(ts)==6:
	# 		y = int(ts[:4])
	# 		m = int(ts[4:])
	# 		start = datetime(y, m, 1)
	# 		end = datetime(y, m, calendar.monthrange(y,m)[1], 23, 59, 59)
	# 	if len(ts)==8:
	# 		y = int(ts[:4])
	# 		m = int(ts[4:6])
	# 		d = int(ts[6:8])
	# 		thirty = timedelta(days=30)
	# 		end = datetime(y, m, d, 23, 59, 59)
	# 		start = end-thirty		
	# else:		
	# 	thirty = timedelta(days=30)
	# 	end = datetime.now()
	# 	start = end-thirty

	return checkuser_query%(get_db_name(wp_pr),wiki_timestamp(start),wiki_timestamp(end))



# wikimedia cluster information extracted from http://noc.wikimedia.org/conf/highlight.php?file=db.php
# NOTE: The default mapping is 's3'
cluster_mapping = {'enwiki':'s1','bgwiki':'s2','bgwiktionary':'s2','cswiki':'s2','enwikiquote':'s2','enwiktionary':'s2','eowiki':'s2','fiwiki':'s2','idwiki':'s2','itwiki':'s2','nlwiki':'s2','nowiki':'s2','plwiki':'s2','ptwiki':'s2','svwiki':'s2','thwiki':'s2','trwiki':'s2','zhwiki':'s2','commonswiki':'s4','dewiki':'s5','frwiki':'s6','jawiki':'s6','ruwiki':'s6','eswiki':'s7','huwiki':'s7','hewiki':'s7','ukwiki':'s7','frwiktionary':'s7','metawiki':'s7','arwiki':'s7','centralauth':'s7','cawiki':'s7','viwiki':'s7','fawiki':'s7','rowiki':'s7','kowiki':'s7'}

#db_mapping = {'s2':'db1018', 's1':'db1033', 's7':'db1024', 's6':'db1040', 's5':'db1021', 's4':'db1004','s3':'db1035}'
# new CNAME system.
# TODO: abstract mapping to a use just number and then autogenerate CNAMES aliases
db_mapping = {'s1':'s1-analytics-slave.eqiad.wmnet', 
	      's2':'s2-analytics-slave.eqiad.wmnet', 
	      's3':'s3-analytics-slave.eqiad.wmnet',
	      's4':'s4-analytics-slave.eqiad.wmnet',
	      's5':'s5-analytics-slave.eqiad.wmnet', 
	      's6':'s6-analytics-slave.eqiad.wmnet',
	      's7':'s7-analytics-slave.eqiad.wmnet', 
	      }

def get_db_name(wp_pr):
	'''Returns the name of the database'''
	return '%swiki'%wp_pr


def get_host_name(wp_pr):
	'''Returns the host name for the wiki project wp_pr'''
	wiki = get_db_name(wp_pr)
	cluster = cluster_mapping.get(wiki, 's3')
	return db_mapping[cluster]

def get_db_connection(wp_pr):
	'''Returns a MySql connection to `wp_pr`, e.g. `en`'''

	host_name = get_host_name(wp_pr)
	db_name = get_db_name(wp_pr)

	db = MySQLdb.connect(host=host_name,read_default_file=os.path.expanduser('~/.my.cnf'))
	logging.info('Connected to [db:%s,host:%s]'%(db_name,host_name))
	return db

def get_cursor(wp_pr,server_side=False):
	'''Returns a server-side cursor

	:arg wp_pr: str, Wikipedia project (e.g. `en`)
	:arg server_side: bool, if True returns a server-side cursor. Default is False
	'''
	db = get_db_connection(wp_pr)
	cur = db.cursor(MySQLdb.cursors.SSCursor) if server_side else db.cursor(MySQLdb.cursors.Cursor)
	
	return cur
