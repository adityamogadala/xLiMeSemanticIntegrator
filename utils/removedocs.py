import pymongo
from pymongo import MongoClient
import datetime
from datetime import date
class TestMongoSize:
	def __init__(self, configdic):
		self.configdict = configdic
	def removeolddocs(self):
		if self.configdict['MongoDBPath']!="":
                        client = MongoClient(self.configdict['MongoDBPath'])
                        if self.configdict['MongoDBUserName']!="" and self.configdict['MongoDBPassword']!="":
                                client.the_database.authenticate(self.configdict['MongoDBUserName'],self.configdict['MongoDBPassword'],source=self.configdict['MongoDBStorage'])
                                storedb = client[self.configdict['MongoDBStorage']]
				collection,collection1,collection2,collection3,collection4 = storedb[self.configdict['KafkaTopicTVMetadata']], storedb[self.configdict["KafkaTopicSocialMedia"]],storedb[self.configdict['KafkaTopicNews']],storedb[self.configdict['KafkaTopicASR']],storedb[self.configdict['KafkaTopicSubtitles']]
				######## Remove Docs ##############
				dt = datetime.datetime.now()
				value = str(dt-datetime.timedelta(days=1)).split()[0]
				value_sub = str(dt-datetime.timedelta(days=0)).split()[0]
				collection.remove( { 'Date': { '$lt': value } } )
				collection1.remove( { 'Date': { '$lt': value} } )
				collection2.remove( { 'Date': { '$lt': value } } )
				collection3.remove( { 'Date': { '$lt': value_sub } } )
				collection4.remove( { 'Date': { '$lt': value_sub } } )
			else:
				print 'Please Set MongoDB UserName Password in Config file.'
		else:
			print 'Please Set MongoDB path in Config file.'
def main():
	configdict={}
        config = '../config/Config.conf'
        with open(config) as config_file:
		for lines in config_file:
			if re.search(r'=',lines):
				key = lines.strip('\n').strip().split('=')
				configdict[key[0]]=key[1]
	testmongo = TestMongoSize(configdict)
	testmongo.removeolddocs()
if __name__ == "__main__":
	main()
