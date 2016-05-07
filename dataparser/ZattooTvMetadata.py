#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import os
import re
from pymongo import MongoClient
import pymongo
import datetime
import sys
import glob
from pprint import pprint

class ZattooToMongo:
	def __init__(self, path, mongodatabase,topics):
		self.path_to_dir = path
		self.mdb = mongodatabase
		self.topic = topics
	def readConfig(self):
		configdict={}
		config = '../config/Config.conf'
		with open(config) as config_file:
			for lines in config_file:
				if re.search(r'=',lines):
					key = lines.strip('\n').split['=']
					configdict[key[0]]=key[1]
		return configdict
	def GenerateTVMetaData(self,jsonfile):
		jsonList = []
		try:
			json_data=open(self.path_to_dir+jsonfile)
			data = json.load(json_data)
			for item in data["@graph"]:
				source = item["@graph"][1]["@id"]
				if (re.search(r'zattoo.com/program', source)):
					try:
						title = item["@graph"][1]["ma:title"].encode('utf-8','replace').decode("utf-8")
					except:
						title = ""
					try:
						description = item["@graph"][1]["ma:description"].encode('utf-8','replace').decode("utf-8")
					except:
						description = ""
					try:
						imagelink = item["@graph"][1]["ma:hasRelatedImage"]["@id"]
					except:
						imagelink =""
					try:
						date1 = item["@graph"][1]["ma:date"]["@value"]
						date12 = date1.split("T")[0]
					except:
                                                date12 = ""
					jsonString = {'Date':str(date12),'SourceURL':source,'ImageSource':imagelink,'Title':title, 'Text':str(description)}
                                        jsonList.append(jsonString)
			json_data.close()
		except:
			pass
		return jsonList
	def MongoData(self):
		files_in_dir = os.listdir(self.path_to_dir)
		configdict=self.readConfig()
		if configdict['MongoDBPath']!="":	
			client = MongoClient(configdict['MongoDBPath'])
			if configdict['MongoDBUserName']!="" and configdict['MongoDBPassword']!="":
				client.the_database.authenticate(configdict['MongoDBUserName'],configdict['MongoDBPassword'],source=self.mdb)
				db = client[self.mdb]
				for file_in_dir in files_in_dir:
						if self.topic == configdict['KafkaTopicTVMetadata']:
							jsonStrings = self.GenerateTVMetaData(file_in_dir)
							tvmetadata = db.tvmetadata
							if len(jsonStrings)!=0:
								for values in jsonStrings:
									try:
										tvmetadata.insert(values,continue_on_error=True)
									except pymongo.errors.DuplicateKeyError:
										pass
			else:
				print 'Please Set MongoDB UserName Password in Config file.'
		else:
			print "Please Set MongoDB path in Config file."
		fil = glob.glob(self.path_to_dir+"*")
		for f in fil:
			os.remove(f)
'''
def main():
	if len(sys.argv)!=3:
		print "Usage: Enter Arg-1) Path to DataStorage  Arg-2) Topic"
		sys.exit()
	path = sys.argv[1]
        topic = sys.argv[2]
	mongo = "VicoStore"
	mongoobject = PushToMongo(path,mongo,topic)
	mongoobject.MongoData()
if  __name__ =='__main__':
	main()
'''
