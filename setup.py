#Description     : Provides Initial Setup.
#Author          : Aditya Mogadala 
#email           : aditya.mogadala@kit.edu
#Version         : 1.0.1
#Copyright       : Institute AIFB, Karlsruhe Institute of Technology (KIT)
#==============================================================================
import re
import os
def readConfig():
	configdict={}
	config = './config/Config.conf'
	with open(config) as config_file:
		for lines in config_file:
			if re.search(r'=',lines):
				key = lines.strip('\n').split('=')
				configdict[key[0]]=key[1]
	return configdict
def createFolders():
	params = readConfig()
	for key,value in params.iteritems():
		if re.search(r'^KafkaTopic',key):
			if not os.path.exists('./storedata/'+value):
				os.makedirs('./storedata/'+value)
def main():
	createFolders()
if __name__ == '__main__':
	main()
