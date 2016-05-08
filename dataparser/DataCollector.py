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
from threading import Thread
class AutomatePushToMongo:
	def __init__(self,path,mongo,confdict):
		self.path = path
		self.mongo = mongo
		self.confdicts = confdict
	def continous_java_run(self,topic):
		if topic==self.confdicts['KafkaTopicSubtitles']:
			tot = "java -cp ../utils/kafkaextractor_largest.jar:. aifb.kit.xlime.kafkaextracor.RunExtractor "+topic+" KITCacher1"
		elif topic==self.confdicts['KafkaTopicNews']:
			tot = "java -cp ../utils/kafkaextractor_largest.jar:. aifb.kit.xlime.kafkaextracor.RunExtractor "+topic+" KITCachernew1"
		elif topic==self.confdicts['KafkaTopicSocialMedia']:
			tot = "java -cp ../utils/kafkaextractor.jar:. aifb.kit.xlime.kafkaextracor.RunExtractor "+topic+" KITRestartSocial"
		else:
			tot = "java -cp ../utils/kafkaextractor.jar:. aifb.kit.xlime.kafkaextracor.RunExtractor "+topic+" KITVm3Consumer1"
		vals = commands.getoutput(tot)
		time.sleep(2)
	def continous_mongo_socialmedia(self):
		topic=self.confdicts['KafkaTopicSocialMedia']
		path1=self.path+topic+"/"
	        sm = VicoSocialMediaStream.SocialMediaToMongo(path1,self.mongo,topic)
        	sm.MongoData()
	def continous_mongo_zattoosub(self):
		topic=self.confdicts['KafkaTopicSubtitles']
		path1=self.path+topic+"/"
	        sm = SubtitlesProcessing.PushToMongoSpeech(path1,self.mongo,topic)
        	sm.MongoData()
	def continous_mongo_zattooepg(self):
		topic=self.confdicts['KafkaTopicTVMetadata']
		path1=self.path+topic+"/"
	        metadata = ZattooTvMetadata.ZattooToMongo(path1,self.mongo,topic)
        	metadata.MongoData()
	def continous_mongo_news(self):
		topic=self.confdicts['KafkaTopicNews']
		path1=self.path+topic+"/"
	        news = JsiNewsStream.Producer(path1,self.mongo,topic)
        	news.run()
##### Add more here to support different types of data #######################
		 
def main():
        path = '../storedata/'
	configdict={}
	config = '../config/Config.conf'
	with open(config) as config_file:
		for lines in config_file:
			if re.search(r'=',lines):
				key = lines.strip('\n').strip().split('=')
				configdict[key[0]]=key[1]
        mongostore = configdict['MongoDBStorage']
	generatedata = AutomatePushToMongo(path,mongostore,configdict)
	try:
   		t1 = Thread(target=generatedata.continous_java_run, args=(confdict['KafkaTopicSocialMedia'],))
		t1.start()
   		t2 = Thread(target=generatedata.continous_java_run, args=(confdict['KafkaTopicTVMetadata'],))
		t2.start()
   		t0 = Thread(target=generatedata.continous_java_run, args=(confdict['KafkaTopicNews'],))
		t0.start()
   		t6 = Thread(target=generatedata.continous_java_run, args=(confdict['KafkaTopicSubtitles'],))
		t6.start()
		t3 = Thread(target=generatedata.continous_mongo_socialmedia)
		t3.start()
		t4=Thread(target=generatedata.continous_mongo_zattooepg)
		t4.start()
		t5=Thread(target=generatedata.continous_mongo_news)
		t5.start()
		t7=Thread(target=generatedata.continous_mongo_zattoosub)
		t7.start()
##### Add more here to support different types of data #######################
	except:
		pass	
if  __name__ =='__main__':
        main()
