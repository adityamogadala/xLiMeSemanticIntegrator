# -*- coding: utf-8 -*-
#==============================================================================
#Description     : Get and convert Subtitles data in RDF format to JSON from Kafka and push it to MongoDB.
#Author          : Aditya Mogadala 
#email           : aditya.mogadala@kit.edu
#Version         : 1.0.1
#Copyright       : Institute AIFB, Karlsruhe Institute of Technology (KIT)
#==============================================================================
import json
import os
import re
import sys
import glob
from pymongo import MongoClient
import pymongo
class PushToMongoSubtitles:
	def __init__(self, path,mongo,topics):
		self.path_to_dir = path
		self.topic = topics
		self.mdb = mongo
	def readConfig(self):
		configdict={}
                config = '../config/Config.conf'
                with open(config) as config_file:
                        for lines in config_file:
				if re.search(r'=',lines):
                                	key = lines.strip('\n').split('=')
                                	configdict[key[0]]=key[1]
		return configdict
	def GenerateZattooSub(self,jsonfile):
		jsonList = []
		newjsondict = {}
		try:
			json_data=open(self.path_to_dir+jsonfile)
			data = json.load(json_data)
			for item in data["@graph"]:
				jsonString={}
				for items in item["@graph"]:
						source = items["@id"]
						zattoosource=""
						if (re.search(r'zattoo.com/processed', source)):
								zattoosource=source
						if zattoosource!="":
							jsonString['SourceURL']=zattoosource
						if "xlime:hasSubittleText" in items:
							speech = items["xlime:hasSubittleText"]
							jsonString['SubtitlesToText']=" ".join(speech.replace("\n"," ").split())
						if "xlime:hasStartTime" in items:
                                                        stime = items["xlime:hasStartTime"]["@value"]
                                                        jsonString['StartTime']=stime
						if "xlime:hasStreamPosition" in items:
							stamp = items["xlime:hasStreamPosition"]["@value"]
							jsonString['StreamPosition']=stamp
						if "xlime:hasZattooCID" in items:
							ChannelID = items["xlime:hasZattooCID"]
							jsonString['CID']=ChannelID
                                                if "rdfs:label" in items:
                                                        lang = items["rdfs:label"]
							if lang=="de" or lang=="it" or lang=="en" or lang=="es":
	                                                        jsonString['Lang']=lang
				jsonList.append(jsonString)
			json_data.close()
		except:
			pass
		if jsonList!=None and len(jsonList)!=0:
				if 'CID' in jsonList[0]:
					newjsondict['CID']=jsonList[0]['CID']
				else:
					newjsondict['CID']=0
				if 'StreamPosition' in jsonList[0]:
					newjsondict['StreamPosition']=jsonList[0]['StreamPosition']
				else:
					newjsondict['StreamPosition']=0.0
			        if 'StartTime' in jsonList[0]:
                                        newjsondict['StartTime']=jsonList[0]['StartTime']
					newjsondict['Date'] = jsonList[0]['StartTime'].split("T")[0]
                                else:
                                        newjsondict['StartTime']=""
                                        newjsondict['Date']=""
                                if 'Lang' in jsonList[0]:
                                        newjsondict['Lang']=jsonList[0]['Lang'].encode('utf-8','replace').decode("utf-8")
                                else:
                                        newjsondict['Lang']=""
				newjsondict['SubtitlesToText']=jsonList[0]['SubtitlesToText'].encode('utf-8','replace').decode("utf-8")
				newjsondict['SourceURL']=jsonList[1]['SourceURL']
				jsonList=[]
				jsonList.append(newjsondict)
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
						if self.topic == configdict["KafkaTopicSubtitles"]:
							jsonStrings = self.GenerateZattooSub(file_in_dir)
							mongocoll = str(configdict["KafkaTopicSubtitles"])
							bulk = db.mongocoll
							if jsonStrings!=None:
								for values in jsonStrings:
									if 'SourceURL' in values and 'SubtitlesToText' in values:
										zattooid = values["SourceURL"].split("/")[-1].strip()
										if int(zattooid) > 0:
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