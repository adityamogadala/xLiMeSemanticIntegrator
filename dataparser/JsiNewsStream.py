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
              #  client = KafkaClient("aifb-ls3-hebe.aifb.kit.edu:9092")
               # producer = SimpleProducer(client)
		#newslist= ["www.20min.ch","www.tagesanzeiger.ch","www.bernerzeitung.ch","www.bazonline.ch","www.derbund.ch","www.lematin.ch","www.tdg.ch","www.24heures.ch","www.solothurnerzeitung.ch","www.aargauerzeitung.ch","www.grenchnertagblatt.ch","www.limmattalerzeitung.ch","www.bzbasel.ch","www.basellandschaftlichezeitung.ch"]
		#client1 = MongoClient()
                #client1.the_database.authenticate('vicouser', 'testrun', source=self.mongo)
                #db1 = client1[self.mongo]
		#bulk1 = db1.newstvrec
                #if len(messagelist)!=0:
                 #       for message in messagelist:
		#		try:
		#			bulk1.insert(json.loads(message),continue_on_error=True)
		#		except pymongo.errors.DuplicateKeyError:
		#			pass
		#		try:
	         #                       urllink = json.loads(message)["jsinewslink"].split("/")[2]
		#			if urllink.strip() in newslist:
	        #	                        producer.send_messages('KITNewsTVRecommender', message)
        	 #       	                time.sleep(1)
		#		except:
		#			pass
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
		client = MongoClient('aifb-ls3-merope.aifb.kit.edu',27017)
		client.the_database.authenticate('vicouser', 'testrun', source=self.mdb)
		db = client[self.mdb]
		for file_in_dir in files_in_dir:
				if self.topic == "jsi-newsfeed":
					jsonStrings = self.GenerateNewsFeed(file_in_dir)	
					bulk = db.jsinewsfeed
					if len(jsonStrings)!=0:
						for values in jsonStrings:
							#print values
							try:
								bulk.insert(values,continue_on_error=True)
							except pymongo.errors.DuplicateKeyError:
								pass
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
