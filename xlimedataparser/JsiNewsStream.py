# -*- coding: utf-8 -*-
#==============================================================================
#Description     : Get and convert News data in RDF format to JSON from Kafka and push it to MongoDB.
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
import datetime
import glob
from datetime import date

class Producer:
        def __init__(self, path,configdic,topics):
                self.path = path
                self.configdic = configdic
                self.topic = topics
        def run(self):
                mongoobject = NewsToMongo(self.path,self.configdic,self.topic)
                mongoobject.MongoData()
class NewsToMongo:
	def __init__(self, path,configdic,topics):
		self.path_to_dir = path
		self.configdict = configdic
		self.topic = topics
	def GenerateNewsFeed(self,jsonfile):
		jsonList = []
		try:
			json_data=open(self.path_to_dir+jsonfile)
			data = json.load(json_data)
			for item in data["@graph"]:
				for items in item["@graph"]:
					if re.search(r'ijs.si/article',items["@id"]):
						try:
							dateitem = items["dcterms:created"]["@value"]
							x1 = dateitem.split('T')[0].split('-')
							x2 = dateitem.split('T')[1].split(':')
							finaldate = datetime.datetime(int(x1[0]),int(x1[1]),int(x1[2]),int(x2[0]),int(x2[1]),int(float(x2[2])))
						except:
							finaldate=""
						try:
							language = items["dcterms:language"]
						except:
							language=""
						try:
							source = items["dcterms:source"]["@id"]
						except:
							source=""
						try:
							modified = re.sub('\n',' ',items["sioc:content"])
							text = re.sub('\s{2,}',' ',modified.strip())
							text = str(text.encode('utf-8', 'replace'))
						except:
							text=""
						try:
							title = re.sub('\n',' ',items["dcterms:title"])
							title1 = re.sub('\s{2,}',' ',title.strip())
						except:
							title1=""
						jsonString = {'Date':str(finaldate),'SourceURL':source,'Title': str(title1.encode('utf-8', 'replace')), 'Text':text, 'Lang': language}
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
						if self.topic == self.configdict['KafkaTopicNews']:
							jsonStrings = self.GenerateNewsFeed(file_in_dir)
							mongocoll = str(self.configdict['KafkaTopicNews'])	
							bulk = db.mongocoll
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
