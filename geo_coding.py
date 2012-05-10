'''

ETL for geo coding entries from the recentchanges table. 

1. Country, total editors, total active editors (5+), total very active editors (100+)
2. Country, top 10 cities, percentage of total edits from each city  


'''

import sys,os
import gzip
import operator

# if one sadly is not allowed to install python pagages....
# sys.path.append('pygeoip/')
import pygeoip

# Path to Geo IP database
geoIP_fn = ' /usr/share/GeoIP/GeoIPCity.dat'

gi = pygeoip.GeoIP(geoIP_fn, pygeoip.MEMORY_CACHE)

# test
# print gi.record_by_addr('178.192.86.113')

### EXTRACT
def extract(fn_in,compressed=False):
	'''Extracts geo data on editor and country/city level from the data source.

	The source is a compressed mysql result set with the following format.

	for s in source:
		s[0] == user_name
		s[1] == ip address
		s[2] == len changed

	:arg source: iterable
	:returns: (editors,countries)
	'''

	editors = {}
	countries = {}

	if not os.path.exists(fn_in):
		print "Warning: %s doesn't exist."%fn_in
		return (editors,countries)


	if compressed:
		source =  gzip.open(fn_in, 'r')
	else:
		source =  open(fn_in, 'r')

	# discard the headers!
	source.readline()

	for line in source:
		res = line[:-1].split('\t')
		
		user = res[0]
		ip = res[1]
		try:
			len_change = int(res[2])
		except:
			len_change = 0
		# len_change = int(res[2]) if res[2] else 0

		# geo lookup
		record = gi.record_by_addr(ip)

		if record:

			city = record['city'] 
			country = record['country_name'] 

			if city=='' or city==' ':
				city = "Unknown"


		else:
			# ip invalid
			city = 'Invalid IP'
			country = 'Invalid IP'



		# country -> city data
		if country not in countries:
			countries[country] = {}

		if city in countries[country]:
			countries[country][city] += 1
		else:
			countries[country][city] = 1

		
		# country -> editors data

		if user not in editors:
			editors[user] = {}

		if country in editors[user]:
			editors[user][country]['edits'] += 1
			editors[user][country]['len_change'] += len_change
		else:
			# country not in editors[user]
			editors[user][country] = {}
			editors[user][country]['edits'] = 1
			editors[user][country]['len_change'] = len_change

	return (editors,countries)

### TRANSFORM
def transform(editors):
	'''Aggregates the numbers of number of all,active,very active editors per country

	Returns countries_editors dictionary:

		{country: [all editors, active editors, very active editors]}

	'''

	countries_editors = {}

	for ed, ginfo in editors.items():

		for country , einfo in ginfo.items():

			if country not in countries_editors:
				countries_editors[country] = [0,0,0]

			if einfo['edits'] > 0:
				countries_editors[country][0] +=1 
				if einfo['edits'] >= 5:
					countries_editors[country][1] +=1
					if einfo['edits'] >= 100:
						countries_editors[country][2] +=1 

	return countries_editors

### LOAD
def load(lang,countries_editors,countries_cities,output_dir='.'):
	'''Stores two metrics:

		1. Country, total editors, total active editors (5+), total very active editors (100+)
		2. Country, total number of edits, top 10 cities, percentage of total edits from each city  

	:arg lang: str, language 
	:arg countries_editors: dict, editor info per country
	:arg countries_cities: dict, city info per country 
	'''

	fn = os.path.join(output_dir,'%s_geo_editors.tsv'%lang)
	# f = codecs.open('editor_geo.csv', encoding='utf-8',mode='w')
	f = open(fn,'w')
	sep = '\t'

	for country in sorted(countries_editors.keys()):

		info = countries_editors[country]


		f.write('%s%s%s\n'%(country,sep,sep.join(['%s'%i for i in info])))

	f.close()

	fn = os.path.join(output_dir,'%s_geo_cities.tsv'%lang)
	f = open(fn,'w')


	for country in sorted(countries_cities.keys()):

		cities = countries_cities[country]

		city_info_sorted = sorted(cities.iteritems(),key=operator.itemgetter(1),reverse=True)
		
		totaledits = sum([c[1] for c in city_info_sorted])
		
		city_info_sorted_str = sep.join([ sep.join((c[0] , '%.1f%%'%(100.*c[1]/totaledits))) for c in city_info_sorted[:100]])


		pre = '%s%s%s'%(country,sep,totaledits)

		f.write('%s%s%s\n'%(pre,sep,city_info_sorted_str))







