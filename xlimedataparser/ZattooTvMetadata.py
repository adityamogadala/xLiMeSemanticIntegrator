# -*- coding: utf-8 -*-
#==============================================================================
#Description     : Get and convert TV Meta data in RDF format to JSON from Kafka and push it to MongoDB.
#Author          : Aditya Mogadala 
#email           : aditya.mogadala@kit.edu
#Version         : 1.0.1
#Copyright       : Institute AIFB, Karlsruhe Institute of Technology (KIT)
#==============================================================================
import json
import os
import re
from pymongo import MongoClient
import pymongo
import glob

class ZattooToMongo:
	def __init__(self,path,configdic,topics):
		self.path_to_dir = path
		self.configdict = configdic
		self.topic = topics
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
		if self.configdict['MongoDBPath']!="":	
			client = MongoClient(self.configdict['MongoDBPath'])
			if self.configdict['MongoDBUserName']!="" and self.configdict['MongoDBPassword']!="":
				client.the_database.authenticate(self.configdict['MongoDBUserName'],self.configdict['MongoDBPassword'],source=self.configdict['MongoDBStorage'])
				db = client[self.configdict['MongoDBStorage']]
				for file_in_dir in files_in_dir:
						if self.topic == self.configdict['KafkaTopicTVMetadata']:
							jsonStrings = self.GenerateTVMetaData(file_in_dir)
							bulk = db[self.configdict['KafkaTopicTVMetadata']]
							if len(jsonStrings)!=0:
								for values in jsonStrings:
									try:
										bulk.insert(values,continue_on_error=True)
									except pymongo.errors.DuplicateKeyError:
										pass
			else:
				print 'Please Set MongoDB UserName Password in Config file.'
		else:
			print "Please Set MongoDB path in Config file."
		fil = glob.glob(self.path_to_dir+"*")
		for f in fil:
			os.remove(f)
