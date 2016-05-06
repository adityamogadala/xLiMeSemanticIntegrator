# -*- coding: utf-8 -*-
import json
import os
import re
import sys
import glob
import rake
from pymongo import MongoClient
import pymongo
import pprint
import arrow
class PushToMongoSpeech:
	def __init__(self, path, topics,mongo):
		self.path_to_dir = path
		self.topic = topics
		self.mdb = mongo
	def GenerateZattooAsr(self,jsonfile):
		jsonList = []
		newjsondict = {}
		try:
			json_data=open(self.path_to_dir+jsonfile)
			data = json.load(json_data)
			for item in data["@graph"]:
				jsonString={}
				for items in item["@graph"]:
#						pprint.pprint(items)
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
	        client = MongoClient('aifb-ls3-merope.aifb.kit.edu',27017)
	        client_lei = MongoClient('mongodb://aifb-ls3-remus.aifb.kit.edu:19005') #for lei semantic web challange (ISWC 2015)
                client.the_database.authenticate('vicouser', 'testrun', source=self.mdb)
                db = client[self.mdb]
		db_lei = client_lei['ABIRS']   #for lei semantic web challange (ISWC 2015)
		storelist=[]
		for file_in_dir in files_in_dir:
				if self.topic == "zattoo-asr":
					jsonStrings = self.GenerateZattooAsr(file_in_dir)
					bulk = db.zattooasr
					bulk_lei = db_lei.zattooasr
					#bulk_lei_2 = db_lei.zattooasr2
					#bulk_lei_3 = db_lei.zattooasr3
					if jsonStrings!=None:
						for values in jsonStrings:
							if 'SourceURL' in values and 'SpeechToText' in values:
								#print values
								zattooid = values["SourceURL"].split("/")[-1].strip()
								if int(zattooid) > 0 :
									text = values['SpeechToText']
							        	rake1 = rake.Rake("SmartStoplist.txt")
    									keywords = rake1.run(text)
									try:
								 		 storelist.append(str(values['CID'])+"\t\t"+str(values['StartTime'])+"\t\t"+str(values['PDSource'])+"\t\t"+str(values['PDTitle'])+"\t\t"+str(values['PDStartTime'])+"\t\t"+str(values['SourceURL'])+"\t\t"+str(values['StreamPosition'])+"\t\t"+str(keywords[0][0].encode('utf-8', 'replace'))+"\t\t"+str(keywords[1][0].encode('utf-8', 'replace'))+"\t\t"+str(keywords[2][0].encode('utf-8', 'replace'))+"\t\t"+str(values['Date'])+"\t\t"+str(values['Lang']))
									except:
										pass
									try:
										bulk.insert(values,continue_on_error=True)
									except pymongo.errors.DuplicateKeyError:
										pass
									#for lei semantic web challange (ISWC 2015)  --start
									start = 1000*int(arrow.get(values["StartTime"].strip()).datetime.strftime("%s"))
			                                                end = start + 40000
                        			                        watch_url = "http://zattoo.com/watch/"+values["CID"].strip()+"/"+str(zattooid)+"/"+str(start)+"/"+str(end)
									values['ZattooURL'] = watch_url
									try:
										bulk_lei.insert(values,continue_on_error=True)
									except pymongo.errors.DuplicateKeyError:
										pass
									#try:
									#	bulk_lei_2.insert(values,continue_on_error=True)
									#except pymongo.errors.DuplicateKeyError:
									#	pass
									#try:
									#	bulk_lei_3.insert(values,continue_on_error=True)
									#except pymongo.errors.DuplicateKeyError:
									#	pass
									#for lei semantic web challange (ISWC 2015)  --end

		fil = glob.glob(self.path_to_dir+"*")
		for f in fil:
			os.remove(f)
		return storelist
'''
def main():
	if len(sys.argv)!=3:
		print "Usage: Enter Arg-1) Path to DataStorage  Arg-2) Topic"
		sys.exit()
	path = sys.argv[1]
        topic = sys.argv[2]
	mongo = "VicoStore"
	mongoobject = PushToMongoSpeech(path,topic,mongo)
	j = mongoobject.MongoData()
	for i in j:
		print i
if  __name__ =='__main__':
	main()
'''
