'''

ETL for geo coding entries from the recentchanges table. 

1. Country, total editors, total active editors (5+), total very active editors (100+)
2. Country, top 10 cities, percentage of total edits from each city  


'''

import sys,os,logging
import gzip,codecs,json
import operator

import pygeoip

### EXTRACT
def extract(source,filter_ids,geoIP_fn,sep=None):
	'''Extracts geo data on editor and country/city level from the data source.

	The source is a compressed mysql result set with the following format.

	for s in source:
		s[0] == user_name
		s[1] == ip address
		s[2] == len changed

	:arg source: iterable
	:arg filter_ids: set, containing user id that should be filtered, e.g. bots. Set can be empty in which case nothing will be filtered.
	:arg geoIP_fn: str, path to Geo IP database
	:arg sep: str, separator for elements in source if they are strings. If None, elemtns won't be split
	:returns: (editors,cities)
	'''
	gi = pygeoip.GeoIP(geoIP_fn, pygeoip.MEMORY_CACHE)

	# test
	# print gi.record_by_addr('178.192.86.113')

	editors = {}
	cities = {}


	for line in source:				

		# a line can be a tuple from a sql resultset or a '\n' escaped line in a text file
		if sep:
			res = line[:-1].split(sep)
		else:
			res = line
		
		user = res[0]

		# filter!
		if user in filter_ids:
			continue

		ip = res[1]

		# geo lookup
		record = gi.record_by_addr(ip)

		if record:

			city = record['city'] 
			country = record['country_name'] 

			if city=='' or city==' ':
				city = "Unknown"

			if country=='' or country==' ':
				country = "Unknown"


		else:
			# ip invalid
			city = 'Invalid IP'
			country = 'Invalid IP'



		# country -> city data
		if country not in cities:
			cities[country] = {}

		if city in cities[country]:
			cities[country][city] += 1
		else:
			cities[country][city] = 1

		
		# country -> editors data

		if user not in editors:
			editors[user] = {}

		if country in editors[user]:
			editors[user][country]['edits'] += 1
			# editors[user][country]['len_change'] += len_change
		else:
			# country not in editors[user]
			editors[user][country] = {}
			editors[user][country]['edits'] = 1
			# editors[user][country]['len_change'] = len_change

	return (editors,cities)

### TRANSFORM
def transform(editors,countries,top_cities=10):
	'''Transfrom step for both metrics.

	Aggregates the numbers of number of all,active,very active editors per country. Returns countries_editors dictionary:

		{country: {"all":xx, "5+":xx, "100+":xx}}

	Aggregates the number of edits per country and compiles a list of top cities with a relative weight compared to the most important city (which has a weight of 10)
		{country: {"edits":xx, "top_cities":[(top_city_1,10.0), (top_city_2,xx), (top_city_3,xx)...]}}

	:arg top_cities: Int, number of ranked cities. Optional, default 10

	'''

	### Editor activity

	countries_editors = {}

	countries_editors["World"] = {"all":0,"5+":0,"100+":0}
	for editors, ginfo in editors.iteritems():

		for country , einfo in ginfo.iteritems():

			if country not in countries_editors:
				countries_editors[country] = {"all":0,"5+":0,"100+":0}

			if einfo['edits'] > 0:				
				countries_editors[country]["all"] +=1 
				countries_editors["World"]["all"] += 1
				if einfo['edits'] >= 5:
					countries_editors[country]["5+"] +=1
					countries_editors["World"]["5+"] += 1
					if einfo['edits'] >= 100:
						countries_editors[country]["100+"] +=1 
						countries_editors["World"]["100+"] += 1


	### City rankings

	countries_cities = {}
	for country,cities in countries.iteritems():
		
		countries_cities[country] = {}

		city_info_sorted = sorted(cities.iteritems(),key=operator.itemgetter(1),reverse=True)	
		totaledits = sum([c[1] for c in city_info_sorted])
		countries_cities[country]["edits"] = totaledits
 		### calculating percentages
		# city_info_sorted_str = sep.join([ sep.join((c[0] , '%.1f%%'%(100.*c[1]/totaledits))) for c in city_info_sorted[:top_cities]])
		### pseudo-confuscation for 1 to 10 scale
		city_info_sorted_aggr = [ (c[0] , (10.*c[1]/city_info_sorted[0][1])) for c in city_info_sorted[:top_cities]]
		
		countries_cities[country]["top_cities"] = city_info_sorted_aggr
		

		

	return countries_editors,countries_cities

### LOAD
def load(wp_pr,countries_editors,countries_cities,output_dir='.',ts=None,sep = '\t'):
	'''Stores two metrics in tsv format:

		1. Country, total editors, total active editors (5+), total very active editors (100+)
		2. Country, total number of edits, top 10 cities, percentage of total edits from each city  

	The same data is also stored in json format.

	:arg wp_pr: str, wikipedia project 
	:arg countries_editors: dict, editor info per country
	:arg countries_cities: dict, city info per country 
	:arg sep: str, separator used for datafile. Optional, default '\\t'.
	'''
	def get_filename(base,wp_pr,ts):
		if ts:
			return '_'.join([wp_pr,ts,base])
		else:
			return '_'.join([wp_pr,base])

	# Editor activity per country
	fn = os.path.join(output_dir,get_filename('geo_editors.json',wp_pr,ts))
	f = codecs.open(fn, encoding='utf-8',mode='w')
	countries_editors_json = {}
	countries_editors_json['project'] = wp_pr
	countries_editors_json['world'] = countries_editors["World"]
	countries_editors_json['countries'] = countries_editors
	json.dump(countries_editors_json,f,sort_keys=True,ensure_ascii=False)

	fn = os.path.join(output_dir,get_filename('geo_editors.tsv',wp_pr,ts))
	f = codecs.open(fn, encoding='utf-8',mode='w')
	# f = open(fn,'w')
	for country in sorted(countries_editors.keys()):
		info = countries_editors[country]
		fields = [country,str(info["all"]),str(info["5+"]),str(info["100+"])]
		f.write(sep.join(fields)+'\n')
	f.close()


	# Top contributor cities per country

	fn = os.path.join(output_dir,get_filename('geo_cities.json',wp_pr,ts))
	f = codecs.open(fn, encoding='utf-8',mode='w')
	countries_cities_json = {}
	countries_cities_json['project'] = wp_pr
	countries_cities_json['countries'] = countries_cities
	json.dump(countries_cities_json,f,sort_keys=True,ensure_ascii=False)

	fn = os.path.join(output_dir,get_filename('geo_cities.tsv',wp_pr,ts))
	f = codecs.open(fn, encoding='utf-8',mode='w')
	# f = open(fn,'w')
	for country in sorted(countries_cities.keys()):
		info = countries_cities[country]
		fields = [country,str(info["edits"])]+[sep.join([c_i[0],'%.1f'%c_i[1]]	) for c_i in info["top_cities"]]
		f.write(sep.join(fields)+'\n')

