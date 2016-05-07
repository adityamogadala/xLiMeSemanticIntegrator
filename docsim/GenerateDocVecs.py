# -*- coding: utf-8 -*-
from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import MongoClient
import shelve
import numpy as np
from threading import Thread
import cPickle as pickle
import shutil
import re
class NewsToTvIndex:
        def __init__(self,database):
                self.db = database
	def readConfig(self):
		configdict={}
                config = '../config/Config.conf'
                with open(config) as config_file:
                        for lines in config_file:
				if re.search(r'=',lines):
                                	key = lines.strip('\n').split['=']
                                	configdict[key[0]]=key[1]
		return configdict
	def encorpus(self,corpus_en,corpus_en_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_en = vectorizer.fit_transform(corpus_en).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_en.shape[0]
		languages=["de","it","es"]
		ids = np.array(corpus_en_id).transpose()
		with open("ids_en.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("ids_en.txt","./picklefiles/ids_en.copy")
		for lang in languages:
			s = shelve.open('./StoreWordVec/wiki/en_'+lang+'_vec_wiki_shelf.db')
			with open("docbivec_en.txt","w") as f:
				for ids,row in zip(corpus_en_id,range(rows)):
					doc_vector=np.array([0.0]*100)
					final_vector=np.array([0.0]*100)
					sum_value=0.0
					for fea,value in zip(word_features,X_en[row,:]):
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
			matrix = np.loadtxt("docbivec_en.txt")
			with open('./picklefiles/docbivec_en_'+lang+'.pkl', 'wb') as outfile:
			    pickle.dump(matrix, outfile, pickle.HIGHEST_PROTOCOL)
	def decorpus(self,corpus_de,corpus_de_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_de= vectorizer.fit_transform(corpus_de).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_de.shape[0]
		s = shelve.open('./StoreWordVec/wiki/de_en_vec_wiki_shelf.db')
		with open("docbivec_de.txt","w") as f:
			for ids,row in zip(corpus_de_id,range(rows)):
				doc_vector=np.array([0.0]*100)
				final_vector=np.array([0.0]*100)
				sum_value=0.0
				for fea,value in zip(word_features,X_de[row,:]):
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
		ids = np.array(corpus_de_id).transpose()
		with open("ids_de.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("ids_de.txt","./picklefiles/ids_de.copy")
		matrix = np.loadtxt("docbivec_de.txt")
		with open('./picklefiles/docbivec_de_en.pkl', 'wb') as outfile:
		    pickle.dump(matrix, outfile, pickle.HIGHEST_PROTOCOL)
	def itcorpus(self,corpus_it,corpus_it_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_de= vectorizer.fit_transform(corpus_it).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_de.shape[0]
		s = shelve.open('./StoreWordVec/wiki/it_en_vec_wiki_shelf.db')
		with open("docbivec_it.txt","w") as f:
			for ids,row in zip(corpus_it_id,range(rows)):
				doc_vector=np.array([0.0]*100)
				final_vector=np.array([0.0]*100)
				sum_value=0.0
				for fea,value in zip(word_features,X_de[row,:]):
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
		ids = np.array(corpus_it_id).transpose()
		with open("ids_it.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("ids_it.txt","./picklefiles/ids_it.copy")
		matrix = np.loadtxt("docbivec_it.txt")
		with open('./picklefiles/docbivec_it_en.pkl', 'wb') as outfile:
		    pickle.dump(matrix, outfile, pickle.HIGHEST_PROTOCOL)
	def escorpus(self,corpus_es,corpus_es_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_de= vectorizer.fit_transform(corpus_es).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_de.shape[0]
		s = shelve.open('./StoreWordVec/wiki/es_en_vec_wiki_shelf.db')
		with open("docbivec_es.txt","w") as f:
			for ids,row in zip(corpus_es_id,range(rows)):
				doc_vector=np.array([0.0]*100)
				final_vector=np.array([0.0]*100)
				sum_value=0.0
				for fea,value in zip(word_features,X_de[row,:]):
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
		ids = np.array(corpus_es_id).transpose()
		with open("ids_es.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("ids_es.txt","./picklefiles/ids_es.copy")
		matrix = np.loadtxt("docbivec_es.txt")
		with open('./picklefiles/docbivec_es_en.pkl', 'wb') as outfile:
		    pickle.dump(matrix, outfile, pickle.HIGHEST_PROTOCOL)
        def calcsize(self):
		configdict=self.readConfig()
                if configdict['MongoDBPath']!="":
			client = MongoClient(configdict['MongoDBPath'])
			if configdict['MongoDBUserName']!="" and configdict['MongoDBPassword']!="":
                                client.the_database.authenticate(configdict['MongoDBUserName'],configdict['MongoDBPassword'],source=self.mdb)
                		storedb = client[self.db]
                		collection = storedb["zattooasr"]
                		corpus_en=[]
               			corpus_de=[]
                		corpus_it=[]
                		corpus_es=[]
                		corpus_en_id=[]
                		corpus_de_id=[]
                		corpus_it_id=[]
                		corpus_es_id=[]
                		for p in collection.find():
                             		if p["Lang"]=="en" and p["SpeechToText"]!="":
                                        	corpus_en.append(p["SpeechToText"].strip().strip("\n").encode('utf-8', 'replace').decode("utf-8"))
						corpus_en_id.append(str(p['_id']))
                                	elif p["Lang"]=="de" and p["SpeechToText"]!="":
                                        	corpus_de.append(p["SpeechToText"].strip().strip("\n").encode('utf-8', 'replace'))
						corpus_de_id.append(str(p['_id']))
                                	elif p["Lang"]=="it" and p["SpeechToText"]!="":
                                        	corpus_it.append(p["SpeechToText"].strip().strip("\n").encode('utf-8', 'replace'))
						corpus_it_id.append(str(p['_id']))
                                	elif p["Lang"]=="es" and p["SpeechToText"]!="":
                                        	corpus_es.append(p["SpeechToText"].strip().strip("\n").encode('utf-8', 'replace'))
						corpus_es_id.append(str(p['_id']))

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
				'''
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
				'''
			else:
				print 'Please Set MongoDB UserName Password in Config file.'
		else:
			 print "Please Set MongoDB path in Config file."
		
def main():
        database = "VicoStore"
        testmongo = NewsToTvIndex(database)
        testmongo.calcsize()
if __name__ == "__main__":
        main()
