# -*- coding: utf-8 -*-
import json
import os
import re
from pymongo import MongoClient
import pymongo
import datetime
import sys
#import shutil
import glob
#import pprint
from pprint import pprint
import datetime
from datetime import date
#####################################
from bson.objectid import ObjectId
import shelve
import cPickle as pickle
import numpy as np
from scipy.spatial import distance
from gensim import utils
import arrow

import threading, logging, time
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
#######################################
class Producer:
        def __init__(self, path, mongodatabase,topics):
                self.path = path
                self.mongo = mongodatabase
                self.topic = topics
        def run(self):
                mongoobject = NewsToMongo(self.path,self.mongo,self.topic)
                mongoobject.MongoData()
class NewsToMongo:
	def __init__(self, path, mongodatabase,topics):
		self.path_to_dir = path
		self.mdb = mongodatabase
		self.topic = topics
	def GenerateNewsFeed(self,jsonfile):
		jsonList = []
		try:
			json_data=open(self.path_to_dir+jsonfile)
			data = json.load(json_data)
			for item in data["@graph"]:
				for items in item["@graph"]:
#					pprint(items)
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
		configdict={}
                config = '../config/Config.conf'
                with open(config) as config_file:
                        for lines in config_file:
				if re.search(r'=',lines):
                                	key = lines.strip('\n').split['=']
                                	configdict[key[0]]=key[1]
                if configdict['MongoDBPath']!="":	
			client = MongoClient(configdict['MongoDBPath'])
                        if configdict['MongoDBUserName']!="" and configdict['MongoDBPassword']!="":
                                client.the_database.authenticate(configdict['MongoDBUserName'],configdict['MongoDBPassword'],source=self.mdb)
				db = client[self.mdb]
				for file_in_dir in files_in_dir:
						if self.topic == configdict['KafkaTopicNews']:
							jsonStrings = self.GenerateNewsFeed(file_in_dir)	
							bulk = db.jsinewsfeed
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
'''
def main():
	if len(sys.argv)!=3:
		print "Usage: Enter Arg-1) Path to DataStorage  Arg-2) Topic"
		sys.exit()
	path = sys.argv[1]
        topic = sys.argv[2]
	mongo = "VicoStore"
	mongoobject = Producer(path,mongo,topic)
	mongoobject.run()
if  __name__ =='__main__':
	main()
'''
