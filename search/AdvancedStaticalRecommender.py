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
class XlimeAdvancedRecommender:
	def __init__(self, speechpath, topic, database):
		self.asrpath = speechpath
		self.zattootopic = topic
		self.db = database
	def recommender(self):
		mongoobject = AdvancedSpeechKafkaProcessing.PushToMongoSpeech(self.asrpath,self.zattootopic,self.db)
		queries = mongoobject.MongoData()
		#client = MongoClient('',27017)
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
				storedb = client[self.db]
				collection = storedb["tvmetadata"]
				collection1 = storedb["socialmedia"]
				collection2 = storedb["jsinewsfeed"]
				finallist=[]
				for query in queries:
					rec_zattoo=[]
					rec_vico=[]
					rec_jsi=[]
					dict_py = {}
					vals = query.split("\t\t")
					cursor_tv_1 = collection.aggregate([ { '$match': { '$text': { '$search': vals[7]} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_tv_2 = collection.aggregate([ { '$match': { '$text': { '$search': vals[8]} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_tv_3 = collection.aggregate([ { '$match': { '$text': { '$search': vals[9]} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					for doc,doc1,doc2 in itertools.izip_longest(cursor_tv_1["result"],cursor_tv_2["result"],cursor_tv_3["result"]):
						try:
							rec_zattoo.append(doc["SourceURL"])
						except:
							pass
						try:
							rec_zattoo.append(doc1["SourceURL"])
						except:
							pass
						try:
							rec_zattoo.append(doc2["SourceURL"])
						except:
							pass
					
					cursor_social_1 = collection1.aggregate([ { '$match': { '$text': { '$search': vals[7]} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_social_2 = collection1.aggregate([ { '$match': { '$text': { '$search': vals[8]} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_social_3 = collection1.aggregate([ { '$match': { '$text': { '$search': vals[9]} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
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
					cursor_news_1 = collection2.aggregate([ { '$match': { '$text': { '$search': vals[7]} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_news_2 = collection2.aggregate([ { '$match': { '$text': { '$search': vals[8]} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
					cursor_news_3 = collection2.aggregate([ { '$match': { '$text': { '$search': vals[9]} } },{ '$sort': { 'score': { '$meta': "textScore" } } },{'$limit': 10}])
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
					zatx = vals[5].split("/")
					dict_py["cid"] = vals[0]
					dict_py["zattooid"] = zatx[len(zatx)-1]
					dict_py["streamposition"] = vals[6]
					dict_py["starttime"] = vals[1]
					dict_py["pdstarttime"] = vals[4]
					dict_py["tvmetadatarec"] = rec_zattoo
					if len(rec_vico) >=50:
						dict_py["socialmediarec"] = rec_vico[0:50]
					else:
						dict_py["socialmediarec"] = rec_vico
					if len(rec_jsi) >=50:
						dict_py["jsinewsrec"] = rec_jsi[0:50]
					else:
						dict_py["jsinewsrec"] = rec_jsi
					finallist.append(json.dumps(dict_py))
				return finallist
'''
class Producer(threading.Thread):
    daemon=True
    def run(self):
    	path = "storedata/zattoo-asr/"
	topic = "zattoo-asr"
	database = "VicoStore"
	xlimerec = XlimeRecommender(path,topic,database)
	messagelist = xlimerec.recommender()
        client = KafkaClient("aifb-ls3-hebe.aifb.kit.edu:9092")
        producer = SimpleProducer(client)
#	print messagelist
	for message in messagelist:
#		pprint(message)
		producer.send_messages('KITRecommendations', message)
		time.sleep(1)

def main():
	threads = [Producer()]
	for t in threads:
		t.start()
	time.sleep(5)

if __name__ == "__main__":
	logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
	main()
'''
