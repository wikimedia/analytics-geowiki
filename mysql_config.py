"""Configuration file for the MediaWiki MySql databases.

The method `get_host_name(wp_pr)` returns a host name for the Wikipedia project passed as parameter.

The login info has to to be configured by creating the file `~/.my.cnf` with the following conent:


"""
import os,logging

# check for login credentials
if not os.exists()


# user_name = 'wikiadmin'
# password = 'test'


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