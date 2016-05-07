# -*- coding: utf-8 -*-
from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import MongoClient
import shelve
import numpy as np
from threading import Thread
import cPickle as pickle
import shutil
import re
class SubNewsToTvIndex:
        def __init__(self,database):
                self.db = database
	def encorpus(self,corpus_en,corpus_en_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_en = vectorizer.fit_transform(corpus_en).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_en.shape[0]
		languages=["de","it","es"]
		ids = np.array(corpus_en_id).transpose()
		with open("_sub_ids_en.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("_sub_ids_en.txt","./subtitlepicklefiles/ids_en.copy")
		for lang in languages:
			s = shelve.open('./StoreWordVec/wiki/en_'+lang+'_vec_wiki_shelf.db')
			with open("_sub_docbivec_en.txt","w") as f:
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
			matrix = np.loadtxt("_sub_docbivec_en.txt")
			with open('./subtitlepicklefiles/docbivec_en_'+lang+'.pkl', 'wb') as outfile:
			    pickle.dump(matrix, outfile, pickle.HIGHEST_PROTOCOL)
	def decorpus(self,corpus_de,corpus_de_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_de= vectorizer.fit_transform(corpus_de).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_de.shape[0]
		s = shelve.open('./StoreWordVec/wiki/de_en_vec_wiki_shelf.db')
		with open("_sub_docbivec_de.txt","w") as f:
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
		with open("_sub_ids_de.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("_sub_ids_de.txt","./subtitlepicklefiles/ids_de.copy")
		matrix = np.loadtxt("_sub_docbivec_de.txt")
		with open('./subtitlepicklefiles/docbivec_de_en.pkl', 'wb') as outfile:
		    pickle.dump(matrix, outfile, pickle.HIGHEST_PROTOCOL)
	def itcorpus(self,corpus_it,corpus_it_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_de= vectorizer.fit_transform(corpus_it).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_de.shape[0]
		s = shelve.open('./StoreWordVec/wiki/it_en_vec_wiki_shelf.db')
		with open("_sub_docbivec_it.txt","w") as f:
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
		with open("_sub_ids_it.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("_sub_ids_it.txt","./subtitlepicklefiles/ids_it.copy")
		matrix = np.loadtxt("_sub_docbivec_it.txt")
		with open('./subtitlepicklefiles/docbivec_it_en.pkl', 'wb') as outfile:
		    pickle.dump(matrix, outfile, pickle.HIGHEST_PROTOCOL)
	def escorpus(self,corpus_es,corpus_es_id):
		vectorizer = TfidfVectorizer(min_df=1,encoding="utf-8",decode_error="replace")
		X_de= vectorizer.fit_transform(corpus_es).toarray()
		word_features = vectorizer.get_feature_names()
		rows = X_de.shape[0]
		s = shelve.open('./StoreWordVec/wiki/es_en_vec_wiki_shelf.db')
		with open("_sub_docbivec_es.txt","w") as f:
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
		with open("_sub_ids_es.txt","w") as f:
			for row in ids:
				f.write(row+"\n")
		shutil.copy2("_sub_ids_es.txt","./subtitlepicklefiles/ids_es.copy")
		matrix = np.loadtxt("_sub_docbivec_es.txt")
		with open('./subtitlepicklefiles/docbivec_es_en.pkl', 'wb') as outfile:
		    pickle.dump(matrix, outfile, pickle.HIGHEST_PROTOCOL)
	def alreadyExists(self,collection,url):		
		print collection.find({'Title': 'Latest Bulletin'}).count()
		return (collection.find({'SourceURL': url.strip()}).count())
        def calcsize(self):
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
                		collection = storedb["zattoosub"]
                		collection1 = storedb["tvmetadata"]
                		sub_corpus_en=[]
                		sub_corpus_de=[]
                		sub_corpus_it=[]
                		sub_corpus_es=[]
                		sub_corpus_en_id=[]
                		sub_corpus_de_id=[]
                		sub_corpus_it_id=[]
                		sub_corpus_es_id=[]
                		for p in collection.find():
                        	     		if p["Lang"]=="en" and p["SubtitlesToText"]!="":
							sub_corpus_en.append(p["SubtitlesToText"].strip().strip("\n").encode('utf-8', 'replace').decode("utf-8"))
							sub_corpus_en_id.append(str(p['_id']))
                                		elif p["Lang"]=="de" and p["SubtitlesToText"]!="":
                                        		sub_corpus_de.append(p["SubtitlesToText"].strip().strip("\n").encode('utf-8', 'replace'))
							sub_corpus_de_id.append(str(p['_id']))
                                		elif p["Lang"]=="it" and p["SubtitlesToText"]!="":
                                        		sub_corpus_it.append(p["SubtitlesToText"].strip().strip("\n").encode('utf-8', 'replace'))
							sub_corpus_it_id.append(str(p['_id']))
                                		elif p["Lang"]=="es" and p["SubtitlesToText"]!="":
                                        		sub_corpus_es.append(p["SubtitlesToText"].strip().strip("\n").encode('utf-8', 'replace'))
							sub_corpus_es_id.append(str(p['_id']))
				if len(sub_corpus_en)>0 and len(sub_corpus_en_id)>0:
					try:
						t1 = Thread(target=self.encorpus, args=(sub_corpus_en,sub_corpus_en_id))
		                		t1.start()
					except:
						pass
				if len(sub_corpus_de)>0 and len(sub_corpus_de_id)>0:
					try:
						t2 = Thread(target=self.decorpus, args=(sub_corpus_de,sub_corpus_de_id))
        		       			t2.start()
					except:
						pass
				'''
				if len(sub_corpus_it)>0 and len(sub_corpus_it_id)>0:
					try:
						t3 = Thread(target=self.itcorpus, args=(sub_corpus_it,sub_corpus_it_id))
        		        		t3.start()
					except:
						pass
				if len(sub_corpus_es)>0 and len(sub_corpus_es_id)>0:
					try:
						t4 = Thread(target=self.escorpus, args=(sub_corpus_es,sub_corpus_es_id))
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
        testmongo = SubNewsToTvIndex(database)
        testmongo.calcsize()
if __name__ == "__main__":
        main()
