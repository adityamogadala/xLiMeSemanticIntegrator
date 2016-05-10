# -*- coding: utf-8 -*-
#==============================================================================
#Description     : Get and convert Speech to Text Data in RDF format to JSON from Kafka and push it to MongoDB
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
import rake
from pymongo import MongoClient
import pymongo
import arrow
import time
class PushToMongoSpeech:
	def __init__(self, path, topics,mongo):
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
	def GenerateZattooAsr(self,jsonfile):
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
						if "xlime:hasASRText" in items:
							speech = items["xlime:hasASRText"]
							jsonString['SpeechToText']=speech
						if "xlime:hasStartTime" in items:
                                                        stime = items["xlime:hasStartTime"]["@value"]
                                                        jsonString['StartTime']=stime
						if "xlime:hasStreamPosition" in items:
							stamp = items["xlime:hasStreamPosition"]["@value"]
							jsonString['StreamPosition']=stamp
						if "xlime:hasZattooCID" in items:
							ChannelID = items["xlime:hasZattooCID"]
							jsonString['CID']=ChannelID
						if "xlime:hasPDSource" in items:
                                                        pds = items["xlime:hasPDSource"]
                                                        jsonString['PDSource']=pds
                                                if "xlime:hasPDTitle" in items:
                                                        pdstitle = items["xlime:hasPDTitle"]
                                                        jsonString['PDTitle']=pdstitle
                                                if "rdfs:label" in items:
                                                        lang = items["rdfs:label"]
							if lang=="de" or lang=="it" or lang=="en" or lang=="es":
	                                                        jsonString['Lang']=lang
                                                if "xlime:hasPDStartTime" in items:
                                                        pdstime = items["xlime:hasPDStartTime"]["@value"]
                                                        jsonString['PDStartTime']=pdstime
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
                                if 'PDSource' in jsonList[0]:
                                        newjsondict['PDSource']=jsonList[0]['PDSource']
                                else:
                                        newjsondict['PDSource']=""
                                if 'PDTitle' in jsonList[0]:
                                        newjsondict['PDTitle']=jsonList[0]['PDTitle'].encode('utf-8','replace').decode("utf-8")
                                else:
                                        newjsondict['PDTitle']=""
                                if 'Lang' in jsonList[0]:
                                        newjsondict['Lang']=jsonList[0]['Lang'].encode('utf-8','replace').decode("utf-8")
                                else:
                                        newjsondict['Lang']=""
                                if 'PDStartTime' in jsonList[0]:
                                        newjsondict['PDStartTime']=jsonList[0]['PDStartTime']
                                else:
                                        newjsondict['PDStartTime']=""
				newjsondict['SpeechToText']=jsonList[0]['SpeechToText'].encode('utf-8','replace').decode("utf-8")
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
				storelist=[]
				for file_in_dir in files_in_dir:
						if self.topic == configdict['KafkaTopicASR']:
							jsonStrings = self.GenerateZattooAsr(file_in_dir)
							mongocoll = str(configdict['KafkaTopicASR'])
							bulk = db.mongocoll
							if jsonStrings!=None:
								for values in jsonStrings:
									if 'SourceURL' in values and 'SpeechToText' in values:
										zattooid = values["SourceURL"].split("/")[-1].strip()
										if int(zattooid) > 0 :
											start = 1000*int(arrow.get(values["StartTime"].strip()).datetime.strftime("%s"))
											end = start + 40000
											watch_url = "http://zattoo.com/watch/"+values["CID"].strip()+"/"+str(zattooid)+"/"+str(start)+"/"+str(end)
											values['ZattooURL'] = watch_url
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