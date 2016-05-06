# -*- coding: utf-8 -*-
import pymongo
from pymongo import MongoClient
from gensim import utils
from bson.objectid import ObjectId
import numpy as np
import shelve
import arrow
from scipy.spatial import distance
import cPickle as pickle
import json
class TestMongoSize:
	def __init__(self,database,clientaddress,port):
		self.db = database
		self.address = clientaddress
		self.port = port
	def CompareNewsToZattoo(self,collection2,final_vector,lang1,language):
		reclist = []
		if (lang1=="de"):
			try:
				if (language=="de"):
					with open('./picklefiles/docbivec_'+language+'_en.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
					matrix1 = np.genfromtxt('./picklefiles/ids_de.copy',dtype='str')
				elif (language=="en"):
					with open('./picklefiles/docbivec_'+language+'_de.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
                                        matrix1 = np.genfromtxt('./picklefiles/ids_en.copy',dtype='str')
				zattoo_list = self.ZattooList(final_vector,matrix,matrix1,collection2)
				for values in zattoo_list:
					reclist.append(values)
			except:
				pass
		elif (lang1=="en"):
			try:
	                        if (language=="en"):
					with open('./picklefiles/docbivec_'+language+'_de.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
                                        matrix1 = np.genfromtxt('./picklefiles/ids_en.copy',dtype='str')
				elif (language=="de"):
					with open('./picklefiles/docbivec_'+language+'_en.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
					matrix1 = np.genfromtxt('./picklefiles/ids_de.copy',dtype='str')
				elif (language=="it"):
					with open('./picklefiles/docbivec_'+language+'_en.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
                                        matrix1 = np.genfromtxt('./picklefiles/ids_it.copy',dtype='str')
				elif (language=="es"):
					with open('./picklefiles/docbivec_'+language+'_en.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
                                    	matrix1 = np.genfromtxt('./picklefiles/ids_es.copy',dtype='str')
				zattoo_list = self.ZattooList(final_vector,matrix,matrix1,collection2)
				for values in zattoo_list:
					reclist.append(values)
			except:
				pass
		return reclist
	def ZattooList(self,final_vector,matrix,matrix1,collection2):
		appendlist = []
		a = np.array([final_vector])
                d = distance.cdist(matrix, a, 'correlation')
		sorted_list_ind = np.argsort(d, axis=0)
		sorted_list_val = np.sort(d, axis=0)
		for docs,val in zip(sorted_list_ind[0:10],sorted_list_val[0:10]):
			dic = collection2.find_one({"_id": ObjectId(matrix1[docs[0]])})
			zattoo_dict = {}
			tea = dic["SourceURL"].split("/")[-1].strip()
			#zattoo_dict["zattooid"]=tea
			#zattoo_dict["streamposition"] = dic["StreamPosition"].strip()
			#zattoo_dict["starttime"] = dic["StartTime"].strip()
			zattoo_dict["cid"] = dic["CID"].strip()
			zattoo_dict["simscore"] = 1-val[0]
			start = 1000*int(arrow.get(dic["StartTime"].strip()).datetime.strftime("%s"))
			end = start+40000
			watch_url = "http://zattoo.com/watch/"+dic["CID"].strip()+"/"+str(tea)+"/"+str(start)+"/"+str(end)
			zattoo_dict["ZattooURL"] = watch_url
			if bool(zattoo_dict):
				appendlist.append(zattoo_dict)
		return appendlist
	def calcsize(self):
		client = MongoClient(self.address,self.port)
		client.the_database.authenticate('vicouser', 'testrun', source=self.db)
		storedb = client[self.db]
		collection5 = storedb["queryzattoo"]
		collection2 = storedb["zattooasr"]
		bulk = storedb.returnzattoorec
		if collection5.find().count()>0:
			for p in collection5.find():
				recommends_en=[]
				recommends_de=[]
				recommends_es=[]
				recommends_it=[]
				dict_news_tv = {}
				doc_vector=np.array([0.0]*100)
				final_vector=np.array([0.0]*100)
				if p['Lang']=="de":
					tokens = utils.simple_preprocess(p['Text'].encode('utf-8', 'replace'))
					s = shelve.open('./StoreWordVec/wiki/de_en_vec_wiki_shelf.db')
					final_vec = self.docvectors(s,tokens,doc_vector,final_vector)
					s.close()
					recommends_en = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"en")
					recommends_de = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"de")
				elif p['Lang']=="en":
					tokens = utils.simple_preprocess(p['Text'])
					s = shelve.open('./StoreWordVec/wiki/en_de_vec_wiki_shelf.db')
					final_vec = self.docvectors(s,tokens,doc_vector,final_vector)
        	                        s.close()
                	                recommends_en = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"en")
                        	        recommends_de = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"de")
                                	s = shelve.open('./StoreWordVec/wiki/en_es_vec_wiki_shelf.db')
					final_vec = self.docvectors(s,tokens,doc_vector,final_vector)
        	                        s.close()
					recommends_es = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"es")
                        	        s = shelve.open('./StoreWordVec/wiki/en_it_vec_wiki_shelf.db')
					final_vec = self.docvectors(s,tokens,doc_vector,final_vector)
					s.close()
					recommends_it = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"it")
				dict_news_tv["tvrec_en"] = recommends_en
				dict_news_tv["tvrec_de"] = recommends_de
				dict_news_tv["tvrec_es"] = recommends_es
				dict_news_tv["tvrec_it"] = recommends_it
			collection5.remove(None, safe=True)
			try:
				bulk.insert(json.loads(json.dumps(dict_news_tv)),continue_on_error=True)
			except pymongo.errors.DuplicateKeyError:
				pass
	#	return dict_news_tv
	def docvectors(self,shelvedb,tokens,doc_vector,final_vector):
		for token in tokens:
			try:
				existing = shelvedb[str(token)]
				doc_vector+=existing
			except Exception:
				pass
		if len(tokens)!=0.0:
			final_vector = doc_vector/float(len(tokens))
		else:
			final_vector = doc_vector
		return final_vector
def main():
	database = "VicoStore"
	clientaddress = "aifb-ls3-merope.aifb.kit.edu"
	port=27017
	testmongo=TestMongoSize(database,clientaddress,port)
	testmongo.calcsize()
if __name__ == "__main__":
	main()
