import os
import logging

data_suffix = '_geo'
out_suffix = '_geo'

# for testing
# languages = ['pt', 'hi', 'ar']
# ALL
languages = ['en','fr', 'de', 'it', 'ru','gv', 'gu', 'scn', 'wuu', 'cdo', 'sco', 'gd', 'ga', 'gn', 'gl', 'als', 'lg', 'pnb', 'lb', 'szl', 'la', 'ln', 'lo', 'tt', 'tr', 'cbk-zam', 'li', 'lv', 'to', 'tl', 'vec', 'th', 'ti', 'tg', 'te', 'ksh', 'pcd', 'ta', 'yi', 'ceb', 'yo', 'da', 'za', 'bxr', 'dz', 'hif', 'rm', 'dv', 'bar', 'vls', 'koi', 'qu', 'eml', 'kn', 'fiu-vro', 'bpy', 'crh', 'mhr', 'diq', 'el', 'eo', 'mn', 'zh', 'mwl', 'pms', 'ee', 'tpi', 'arz', 'rmy', 'mdf', 'kaa', 'mh', 'arc', 'cr', 'eu', 'et', 'tet', 'es', 'ba', 'roa-tara', 'mus', 'mrj', 'ha', 'ak', 'lad', 'bm', 'new', 'rn', 'ro', 'dsb', 'bn', 'hsb', 'be', 'bg', 'be-x-old', 'uk', 'wa', 'ast', 'wo', 'got', 'jv', 'bo', 'bh', 'bi', 'map-bms', 'hak', 'tum', 'br', 'lmo', 'ja', 'om', 'glk', 'ace', 'ng', 'ilo', 'ty', 'oc', 'kj', 'st', 'tw', 'krc', 'nds', 'os', 'or', 'ext', 'xh', 'ch', 'co', 'simple', 'bjn', 'ca', 'bs', 'ce', 'ts', 'na', 'cy', 'ang', 'cs', 'udm', 'cho', 'cv', 'cu', 've', 'fj', 'ps', 'srn', 'pt', 'sm', 'lt', 'zh-min-nan', 'frr', 'chr', 'frp', 'xal', 'chy', 'pi', 'war', 'pl', 'tk', 'hz', 'hy', 'nrm', 'hr', 'iu', 'pnt', 'ht', 'hu', 'gan', 'bat-smg', 'hi', 'ho', 'kg', 'an', 'bug', 'he', 'mg', 'fur', 'uz', 'ml', 'mo', 'roa-rup', 'mi', 'as', 'mk', 'ur', 'zea', 'mt', 'stq', 'ms', 'mr', 'ug', 'haw', 'my', 'ki', 'pih', 'aa', 'sah', 'ss', 'af', 'tn', 'vi', 'is', 'am', 'vo', 'ii', 'ay', 'ik', 'ar', 'lbe', 'km', 'io', 'av', 'ia', 'az', 'ie', 'id', 'nds-nl', 'pap', 'ks', 'nl', 'nn', 'no', 'pa', 'nah', 'ne', 'lij', 'csb', 'ny', 'nap', 'myv', 'ig', 'pag', 'zu', 'so', 'pam', 'nv', 'sn', 'kab', 'jbo', 'zh-yue', 'fy', 'fa', 'rw', 'ff', 'fi', 'mzn', 'ab', 'ky', 'zh-classical', 'fo', 'bcl', 'ka', 'nov', 'ckb', 'kk', 'sr', 'sq', 'ko', 'sv', 'su', 'kl', 'sk', 'kr', 'si', 'sh', 'kw', 'kv', 'ku', 'sl', 'sc', 'sa', 'sd', 'sg', 'sw', 'se', 'pdc']


def get_done_languages(d,suffix):
	done = []
	for root, dirs, files in os.walk(d):			
		for f in files:				
			
			path = os.path.join(root,f)
			size =  os.path.getsize(path)
			if size <= 50:
				logging.warning('%s is %s bytes small!'%(path,size))
				# os.system('rm %s'%path)
			if f[-3:]=='csv' or f[-6:]=='tsv.gz':
				done.append(f.split(suffix)[0])
	return done

data_done = get_done_languages('./data',data_suffix)
data_todo =  [l for l in languages if l not in data_done][::-1]

output_done = get_done_languages('./output',out_suffix)
output_todo = [l for l in data_done if l not in output_done]

# print 'data_todo: ', data_todo
# print 'output_todo: ',output_todo
