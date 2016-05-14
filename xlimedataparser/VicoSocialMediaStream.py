# -*- coding: utf-8 -*-
#==============================================================================
#Description     : Get and convert Social Media data in RDF format to JSON from Kafka and push it to MongoDB.
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
import datetime
from datetime import date

class SocialMediaToMongo:
	def __init__(self,path,configdic,topics):
		self.path_to_dir = path
		self.configdict = configdic
		self.topic = topics
	def GenerateSocialData(self,jsonfile):
		jsonList = []
		try:
			json_data=open(self.path_to_dir+jsonfile)
			data = json.load(json_data)
			for item in data["@graph"]:
				for items in item["@graph"]:
					try:
						modified = re.sub('\n',' ',items["sioc:content"])
						dateitem = items["dcterms:created"]["@value"]
						x1 = dateitem.split('T')[0].split('-')
						x2 = dateitem.split('T')[1].split(':')
						finaldate = datetime.datetime(int(x1[0]),int(x1[1]),int(x1[2]),int(x2[0]),int(x2[1]),int(x2[2]))
						source = items["dcterms:source"]["@id"]
						language = items["dcterms:language"]
						if (re.search(r'twitter.com',source)):
							publisher = 'Twitter'
						elif (re.search(r'facebook.com',source)):
							publisher = 'Facebook'
						else:
							publisher = 'Article'
						text = re.sub('\s{2,}',' ',modified.strip())
						jsonString = {'Date':str(finaldate),'SourceURL':source,'Publisher':publisher, 'Text':str(text.encode('utf-8', 'replace').decode("utf-8")), 'Lang': language}
						jsonList.append(jsonString)			
					except:
						pass
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
						if self.topic == self.configdict["KafkaTopicSocialMedia"]:
							jsonStrings = self.GenerateSocialData(file_in_dir)
							bulk = db[self.configdict["KafkaTopicSocialMedia"]]
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
