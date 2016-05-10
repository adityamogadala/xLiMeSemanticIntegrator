# -*- coding: utf-8 -*-
#==============================================================================
#Description     : Create speech to text (StoT) snippets vectors.
#Author          : Aditya Mogadala 
#email           : aditya.mogadala@kit.edu
#Version         : 1.0.1
#Copyright       : Institute AIFB, Karlsruhe Institute of Technology (KIT)
#==============================================================================
from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import MongoClient
import shelve
import numpy as np
from threading import Thread
import cPickle as pickle
import shutil
import re
class SubtitlesVectors:
        def __init__(self,database):
                self.mdb = database
	def readConfig(self):
		configdict={}
                config = '../config/Config.conf'
                with open(config) as config_file:
                        for lines in config_file:
				if re.search(r'=',lines):
                                	key = lines.strip('\n').split('=')
                                	configdict[key[0]]=key[1]
		return configdict
	def createpklfiles(self,lang0,lang,corpus_ids,rows,word_features,X):
		s = shelve.open('./StoreWordVec/wiki/'+lang0+'_'+lang+'_vec_wiki_shelf.db')
		with open("_sub_docbivec_"+lang0+".txt","w") as f:
			for ids,row in zip(corpus_ids,range(rows)):
				doc_vector=np.array([0.0]*100)
				final_vector=np.array([0.0]*100)
				sum_value=0.0
				for fea,value in zip(word_features,X[row,:]):
					try:
						existing = s[str(fea)]
						vec = np.array([float(value)*ip for ip in existing])
						sum_value+=value
						doc_vector+=vec
					except Exception:
						pass
				if sum_value!=0.0:
					final_vector = doc_vector/sum_value
				else:
					final_vector = doc_vector
				f.write(" ".join(map(str, final_vector))+"\n")
		s.close()
		matrix = np.loadtxt("_sub_docbivec_"+lang0+".txt")
		with open('./subtitledocvecpkl/docbivec_'+lang0+'_'+lang+'.pkl', 'wb') as outfile:
			pickle.dump(matrix, outfile, pickle.HIGHEST_PROTOCOL)
	def encorpus(self,corpus_en,corpus_en_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_en = vectorizer.fit_transform(corpus_en).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_en.shape[0]
		languages=["de","it","es"]
		for lang in languages:
			self.createpklfiles("en",lang,corpus_en_id,rows,word_features,X_en)
		ids = np.array(corpus_en_id).transpose()
		with open("_sub_ids_en.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("_sub_ids_en.txt","./subtitledocvecpkl/ids_en.copy")
	def decorpus(self,corpus_de,corpus_de_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_de= vectorizer.fit_transform(corpus_de).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_de.shape[0]
		self.createpklfiles("de","en",corpus_de_id,rows,word_features,X_de)
		ids = np.array(corpus_de_id).transpose()
		with open("_sub_ids_de.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("_sub_ids_de.txt","./subtitledocvecpkl/ids_de.copy")
	def itcorpus(self,corpus_it,corpus_it_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_it= vectorizer.fit_transform(corpus_it).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_it.shape[0]
		self.createpklfiles("it","en",corpus_it_id,rows,word_features,X_it)
		ids = np.array(corpus_it_id).transpose()
		with open("_sub_ids_it.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("_sub_ids_it.txt","./subtitledocvecpkl/ids_it.copy")
	def escorpus(self,corpus_es,corpus_es_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_es= vectorizer.fit_transform(corpus_es).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_es.shape[0]
		self.createpklfiles("es","en",corpus_es_id,rows,word_features,X_es)
		ids = np.array(corpus_es_id).transpose()
		with open("_sub_ids_es.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("_sub_ids_es.txt","./subtitledocvecpkl/ids_es.copy")
        def computevectors(self):
		configdict=self.readConfig()
                if configdict['MongoDBPath']!="":
			client = MongoClient(configdict['MongoDBPath'])
			if configdict['MongoDBUserName']!="" and configdict['MongoDBPassword']!="":
                                client.the_database.authenticate(configdict['MongoDBUserName'],configdict['MongoDBPassword'],source=self.mdb)
                		storedb = client[self.mdb]
                		collection = storedb[conficdict['KafkaTopicSubtitles']]
                		corpus_en,corpus_de,corpus_it,corpus_es=[],[],[],[]
                		corpus_en_id,corpus_de_id,corpus_it_id,corpus_es_id=[],[],[],[]
				################## START - Collects Data from MongoDB (Sequential)################
                		for p in collection.find():
                             		if p["Lang"]=="en" and p["SubtitlesToText"]!="":
                                        	corpus_en.append(p["SubtitlesToText"].strip().strip("\n").encode('utf-8', 'replace').decode("utf-8"))
						corpus_en_id.append(str(p['_id']))
                                	elif p["Lang"]=="de" and p["SubtitlesToText"]!="":
                                        	corpus_de.append(p["SubtitlesToText"].strip().strip("\n").encode('utf-8', 'replace'))
						corpus_de_id.append(str(p['_id']))
                                	elif p["Lang"]=="it" and p["SubtitlesToText"]!="":
                                        	corpus_it.append(p["SubtitlesToText"].strip().strip("\n").encode('utf-8', 'replace'))
						corpus_it_id.append(str(p['_id']))
                                	elif p["Lang"]=="es" and p["SubtitlesToText"]!="":
                                        	corpus_es.append(p["SubtitlesToText"].strip().strip("\n").encode('utf-8', 'replace'))
						corpus_es_id.append(str(p['_id']))

				################## END - Collects Data from MongoDB (Sequential)################
				
				################## START - Spawn Threads to create doc vectors pickle files (Parallel)################
				if len(corpus_en)>0 and len(corpus_en_id)>0:
					try:
						t1 = Thread(target=self.encorpus, args=(corpus_en,corpus_en_id))
		                		t1.start()
					except:
						pass
				if len(corpus_de)>0 and len(corpus_de_id)>0:
					try:
						t2 = Thread(target=self.decorpus, args=(corpus_de,corpus_de_id))
        		       			t2.start()
					except:
						pass
				if len(corpus_it)>0 and len(corpus_it_id)>0:
					try:
						t3 = Thread(target=self.itcorpus, args=(corpus_it,corpus_it_id))
        		       			t3.start()
					except:
						pass
				if len(corpus_es)>0 and len(corpus_es_id)>0:
					try:
						t4 = Thread(target=self.escorpus, args=(corpus_es,corpus_es_id))
        		        		t4.start()
					except:
						pass
				################## END - Spawn Threads to create doc vectors pickle files (Parallel)################
			else:
				print 'Please Set MongoDB UserName Password in Config file.'
		else:
			 print "Please Set MongoDB path in Config file."
		
def main():
        database = "VicoStore"
        testmongo = SubtitlesVectors(database)
        testmongo.computevectors()
if __name__ == "__main__":
        main()
