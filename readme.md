# Geo Coding module

This module can be used to generate datasets that aggregate geographical information about editors. The source of the data is the [recentchanges](http://www.mediawiki.org/wiki/Manual:Recentchanges_table) table of the Mediawiki MySql databases. Currently two datasets are generated

* Each row shows the number of editors of certain activity level for a given country. The files are tab seperated, there is one file per Wikipedia project (e.g. enwp).

	`Country, total editors, total active editors (5+), total very active editors (100+)`
	

* Each row shows a country and total number of edits, followed by a list of the top ten cities and the percentage of edits made in that city. The files are tab seperated, there is one file per Wikipedia project (e.g. enwp).

	`Country, total edits, [city, {0.0-10.0} weight compared to largest contributor city]`

## Privacy

One needs access to IP addresses to create geo coded datasets from wikipedia. [Wikipedia's privacy policy](http://wikimediafoundation.org/wiki/Privacy_policy) states that IP addresses are only stored for a limited period of time. The datasets generated by this module do not contain information about indidual editors, all datapoints are aggregated on a city or country level.


## Dependencies

* Python > 2.6
* argparse
* [mySQLdb](http://mysql-python.sourceforge.net/)
* [mysql_config](http://dev.mysql.com/doc/refman/5.0/en/mysql-config.html)
* [pygeoip](https://github.com/appliedsec/pygeoip), API for Maxmind GeoIP databases. Note: `pip install pygeoip` install a non-functioning version, instead `git clone` the repository and run `python setup.py install`.
* [GeoIP City Database](http://www.maxmind.com/app/city) from Mindmind 

## Configuration

### Directories

In 'process_data.py', set the following directories.

<!-- * `data` : intermediate storage of exported mysql data -->
* `output` : generated geo coded data files

### Mysql

Configure access to the mysql databases by configuring the `mysql_config.py` file. The login info has to to be configured by creating the file `~/.my.cnf` with the following conent:

	[client]
	user = USERNAME
	password = PASSWORD

The data is retrieved using a server-side mySQLdb cursor. The tables queried are:
* [checkuser](http://www.mediawiki.org/wiki/Extension:CheckUser) table, `cu_changes` (Main)
* [recentchanges](http://www.mediawiki.org/wiki/Manual:Recentchanges_table) table (Alternative)
* [user_groups](http://www.mediawiki.org/wiki/Manual:User_groups_table) table. For bot classification.

No joins are performed. 

### GeoIP

Point `geo_coding.geoIP_fn` to the GeoIP City Database.

## Usage

**Note**: Any files that already exist in the cofingured `data`/`output` directories will be overwritten. None of the already existing files will be deleted. At the moment no date-specific information is included anywhere in the files or the file names, it is best to run the script with empty directories. 

Simply run:

	python process_data.py

## Todo

* Add date specific information in the data files and the file names
* Tests!
* Create a package 


