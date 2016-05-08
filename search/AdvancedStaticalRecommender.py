# -*- coding: utf-8 -*-
#==============================================================================
#Description     : Search MongoDB collections.
#Author          : Aditya Mogadala 
#email           : aditya.mogadala@kit.edu
#Version         : 1.0.1
#Copyright       : Institute AIFB, Karlsruhe Institute of Technology (KIT)
#==============================================================================
import pymongo
from pymongo import MongoClient
from pprint import pprint
import json
import sys
import threading, logging, time
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
import AdvancedSpeechKafkaProcessing
import datetime
from datetime import date
import itertools
import rake
class XlimeAdvancedRecommender:
	def __init__(self, speechpath, topic, database):
		self.asrpath = speechpath
		self.zattootopic = topic
		self.db = database
	def readConfig(self):
		configdict={}
                config = '../config/Config.conf'
                with open(config) as config_file:
                        for lines in config_file:
                                if re.search(r'=',lines):
                                        key = lines.strip('\n').split('=')
                                        configdict[key[0]]=key[1]
		return configdict
	def recommender(self,docqueries):
		configdict=self.readConfig()
                if configdict['MongoDBPath']!="":
			client = MongoClient(configdict['MongoDBPath'])
			if configdict['MongoDBUserName']!="" and configdict['MongoDBPassword']!="":
                                client.the_database.authenticate(configdict['MongoDBUserName'],configdict['MongoDBPassword'],source=self.mdb)
				storedb = client[self.db]
				collection = storedb[configdict['KafkaTopicASR']]
				collection1 = storedb[configdict['KafkaTopicSocialMedia']]
				collection2 = storedb[configdict['KafkaTopicNews']]
				###################### Add more collections here to retrieve more ################################
				finallist=[]
				for query in docqueries:
					rec_zattoo=[]
					rec_vico=[]
					rec_jsi=[]
					dict_py = {}
					rake1 = rake.Rake("SmartStoplist.txt")
					vals = rake1.run(query)
					val1 = vals[0][0].encode('utf-8', 'replace')
					val2 = vals[1][0].encode('utf-8', 'replace')
					val3 = vals[2][0].encode('utf-8', 'replace')
					cursor_tv_1 = collection.aggregate([ { '$match': { '$text': { '$search': val1} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_tv_2 = collection.aggregate([ { '$match': { '$text': { '$search': val2} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_tv_3 = collection.aggregate([ { '$match': { '$text': { '$search': val3} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					for doc,doc1,doc2 in itertools.izip_longest(cursor_tv_1["result"],cursor_tv_2["result"],cursor_tv_3["result"]):
						try:
							rec_zattoo.append(doc["ZattooURL"])
						except:
							pass
						try:
							rec_zattoo.append(doc1["ZattooURL"])
						except:
							pass
						try:
							rec_zattoo.append(doc2["ZattooURL"])
						except:
							pass
					
					cursor_social_1 = collection1.aggregate([ { '$match': { '$text': { '$search': val1} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_social_2 = collection1.aggregate([ { '$match': { '$text': { '$search': val2} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_social_3 = collection1.aggregate([ { '$match': { '$text': { '$search': val3} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					for doc,doc1,doc2 in itertools.izip_longest(cursor_social_1["result"],cursor_social_2["result"],cursor_social_3["result"]):
						try:
							rec_vico.append(doc["SourceURL"])
						except:
							pass
						try:
							rec_vico.append(doc1["SourceURL"])
						except:
							pass
						try:
							rec_vico.append(doc2["SourceURL"])
						except:
							pass
					cursor_news_1 = collection2.aggregate([ { '$match': { '$text': { '$search': val1} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_news_2 = collection2.aggregate([ { '$match': { '$text': { '$search': val2} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_news_3 = collection2.aggregate([ { '$match': { '$text': { '$search': val3} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					for doc,doc1,doc2 in itertools.izip_longest(cursor_news_1["result"],cursor_news_2["result"],cursor_news_3["result"]):
						try:
							rec_jsi.append(doc["SourceURL"])
						except:
							pass
						try:
							rec_jsi.append(doc1["SourceURL"])
						except:
							pass
						try:
							rec_jsi.append(doc2["SourceURL"])
						except:
							pass
					if len(rec_zattoo) >=20:
						dict_py["tvmetadatarec"] = rec_zattoo[0:20]
					else:
						dict_py["tvmetadatarec"] = rec_zattoo
					if len(rec_vico) >=20:
						dict_py["socialmediarec"] = rec_vico[0:20]
					else:
						dict_py["socialmediarec"] = rec_vico
					if len(rec_jsi) >=20:
						dict_py["jsinewsrec"] = rec_jsi[0:20]
					else:
						dict_py["jsinewsrec"] = rec_jsi
					finallist.append(json.dumps(dict_py))
				return finallist
