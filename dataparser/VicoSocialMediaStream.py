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
#from bson.objectid import ObjectId
from pprint import pprint
import datetime
from datetime import date

class SocialMediaToMongo:
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
                                	key = lines.strip('\n').split('=')
                                	configdict[key[0]]=key[1]
		return configdict
	def GenerateSocialData(self,jsonfile):
		jsonList = []
		try:
			json_data=open(self.path_to_dir+jsonfile)
			data = json.load(json_data)
			for item in data["@graph"]:
				for items in item["@graph"]:
					#pprint(items)
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
		configdict=self.readConfig()
                if configdict['MongoDBPath']!="":
			client = MongoClient(configdict['MongoDBPath'])
			if configdict['MongoDBUserName']!="" and configdict['MongoDBPassword']!="":
                                client.the_database.authenticate(configdict['MongoDBUserName'],configdict['MongoDBPassword'],source=self.mdb)
				db = client[self.mdb]
				for file_in_dir in files_in_dir:
						if self.topic == configdict["KafkaTopicSocialMedia"]:
							jsonStrings = self.GenerateSocialData(file_in_dir)	
							bulk = db.socialmedia
							if len(jsonStrings)!=0:
								for values in jsonStrings:
									tvdate = values["Date"].split()[0].split("-")
                                                        		tvshow = date(int(tvdate[0]),int(tvdate[1]),int(tvdate[2]))
                                                       			datenow = str(datetime.datetime.now()).split()[0].split("-")
                                                        		datetvnow = date(int(datenow[0]),int(datenow[1]),int(datenow[2]))
                                                       			diff_date = abs(datetvnow-tvshow).days
                                                        		if diff_date <=7:
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
	mongoobject = PushToMongo(path,mongo,topic)
	mongoobject.MongoData()
if  __name__ =='__main__':
	main()
'''
