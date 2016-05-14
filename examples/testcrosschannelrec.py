# -*- coding: utf-8 -*-
from pymongo import MongoClient
import re
class CountMongo:
	def __init__(self,configdic):
		self.configdict = configdic
	def count(self):
		if self.configdict['MongoDBPath']!="":
			client = MongoClient(self.configdict['MongoDBPath'])
			if self.configdict['MongoDBUserName']!="" and self.configdict['MongoDBPassword']!="":
				client.the_database.authenticate(self.configdict['MongoDBUserName'],self.configdict['MongoDBPassword'],source=self.configdict['MongoDBStorage'])
				storedb = client[self.configdict['MongoDBStorage']]
				collection = storedb[self.configdict['KafkaTopicTVMetadata']]
	
		print "Total Docs in Collection [TV Metadata], [Social Media],[News],[TV ASR],[TV SubTitles]::", collection.find().count(), collection1.find().count(), collection2.find().count(), collection3.find().count(),collection4.find().count()
		
def main():
	configdict={}
        config = '../config/Config.conf'
        with open(config) as config_file:
                for lines in config_file:
                        if re.search(r'=',lines):
                                key = lines.strip('\n').strip().split('=')
                                configdict[key[0]]=key[1]
	testmongo = CountMongo(configdict)
	testmongo.count()
if __name__ == "__main__":
	main()
