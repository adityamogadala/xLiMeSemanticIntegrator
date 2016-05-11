# -*- coding: utf-8 -*-
#==============================================================================
#Description     : Call all types of data collectors
#Author          : Aditya Mogadala 
#email           : aditya.mogadala@kit.edu
#Version         : 1.0.1
#Copyright       : Institute AIFB, Karlsruhe Institute of Technology (KIT)
#==============================================================================
import commands
import time
import sys
import VicoSocialMediaStream
import ZattooTvMetadata
import JsiNewsStream
import SubtitlesProcessing
import AdvancedSpeechKafkaProcessing
from threading import Thread
from os.path import expanduser
class AutomatePushToMongo:
	def __init__(self,path,confdic):
		self.path = path
		self.configdict = confdic
	def continous_java_run(self,topic):
		tot = "java -cp ../utils/kafkaextractor_smallest.jar:. aifb.kit.xlime.kafkaextracor.RunExtractor "+topic+" "+self.configdict['KafkaConsumerGroupID']+" "+self.configdict['KafkaZookeeperPath']   # For using smallest offset of Kafka
		#tot = "java -cp ../utils/kafkaextractor_largest.jar:. aifb.kit.xlime.kafkaextracor.RunExtractor "+topic+" "+self.configdict['KafkaConsumerGroupID']+" "+self.configdict['KafkaZookeeperPath']   # For using largest offset of Kafka
		vals = commands.getoutput(tot)
		time.sleep(2)
	def continous_mongo_socialmedia(self):
		topic=self.configdict['KafkaTopicSocialMedia']
		path1=self.path+topic+"/"
	        sm = VicoSocialMediaStream.SocialMediaToMongo(path1,self.configdict,topic)
        	sm.MongoData()
	def continous_mongo_zattoosub(self):
		topic=self.configdict['KafkaTopicSubtitles']
		path1=self.path+topic+"/"
	        sm = SubtitlesProcessing.PushToMongoSubtitles(path1,self.configdict,topic)
        	sm.MongoData()
	def continous_mongo_zattooepg(self):
		topic=self.configdict['KafkaTopicTVMetadata']
		path1=self.path+topic+"/"
	        metadata = ZattooTvMetadata.ZattooToMongo(path1,self.configdict,topic)
        	metadata.MongoData()
	def continous_mongo_zattooasr(self):
		topic=self.configdict['KafkaTopicASR']
		path1=self.path+topic+"/"
	        asrdata = AdvancedSpeechKafkaProcessing.PushToMongoSpeech(path1,self.configdict,topic)
        	asrdata.MongoData()
	def continous_mongo_news(self):
		topic=self.configdict['KafkaTopicNews']
		path1=self.path+topic+"/"
	        news = JsiNewsStream.Producer(path1,self.configdict,topic)
        	news.run()
##### Add more here to support different types of data #######################
		 
def main():
	home = expanduser("~")
        path = home+'/storedata/'
	configdict={}
	config = '../config/Config.conf'
	with open(config) as config_file:
		for lines in config_file:
			if re.search(r'=',lines):
				key = lines.strip('\n').strip().split('=')
				configdict[key[0]]=key[1]
	generatedata = AutomatePushToMongo(path,configdict)
	try:
   		t1 = Thread(target=generatedata.continous_java_run, args=(confdict['KafkaTopicSocialMedia'],))
		t1.start()
   		t2 = Thread(target=generatedata.continous_java_run, args=(confdict['KafkaTopicTVMetadata'],))
		t2.start()
   		t0 = Thread(target=generatedata.continous_java_run, args=(confdict['KafkaTopicNews'],))
		t0.start()
   		t6 = Thread(target=generatedata.continous_java_run, args=(confdict['KafkaTopicSubtitles'],))
		t6.start()
   		t8 = Thread(target=generatedata.continous_java_run, args=(confdict['KafkaTopicASR'],))
		t8.start()
		t3 = Thread(target=generatedata.continous_mongo_socialmedia)
		t3.start()
		t4=Thread(target=generatedata.continous_mongo_zattooepg)
		t4.start()
		t5=Thread(target=generatedata.continous_mongo_news)
		t5.start()
		t7=Thread(target=generatedata.continous_mongo_zattoosub)
		t7.start()
		t9=Thread(target=generatedata.continous_mongo_zattooasr)
		t9.start()
##### Add more here to support different types of data #######################
	except:
		pass	
if  __name__ =='__main__':
        main()
