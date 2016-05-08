# -*- coding: utf-8 -*-
#Description     : Call Search MongoDB collections.
#Author          : Aditya Mogadala 
#email           : aditya.mogadala@kit.edu
#Version         : 1.0.1
#Copyright       : Institute AIFB, Karlsruhe Institute of Technology (KIT)
#==============================================================================
import commands
import time
import sys
import AdvancedStaticalRecommender
import threading, logging, time
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
import pymongo
from pymongo import MongoClient
class RecFromMongo:
	def continous_java_run(self,topic):
		tot = "java -cp ../utils/kafkaextractor.jar:. aifb.kit.xlime.kafkaextracor.RunExtractor "+topic+" KITVm3Consumer"
		vals = commands.getoutput(tot)
class Producer(threading.Thread):
    daemon=True
    def run(self):
        path = "../storedata/zattoo-asr/"
        topic = "zattoo-asr"
        database = "VicoStore"
        xlimerec = AdvancedStaticalRecommender.XlimeAdvancedRecommender(path,topic,database)
        messagelist = xlimerec.recommender()
        client = KafkaClient("aifb-ls3-hebe.aifb.kit.edu:9092")
        producer = SimpleProducer(client)
#        print messagelist
        for message in messagelist:
#               pprint(message)
                producer.send_messages('KITRecommendations', message)
                time.sleep(1)

def main():
	generatedata = RecFromMongo()
	generatedata.continous_java_run("zattoo-asr")
	time.sleep(8)
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
