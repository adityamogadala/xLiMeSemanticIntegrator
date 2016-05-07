# -*- coding: utf-8 -*-
import pymongo
from pymongo import MongoClient
#from pymongo import Connection
from gensim import utils
from bson.objectid import ObjectId
import numpy as np
import shelve
import arrow
from scipy.spatial import distance
import cPickle as pickle
import json
import time
import random
class TestMongoSize:
	def __init__(self,database,clientaddress,port):
		self.db = database
		self.address = clientaddress
		self.port = port
	def readConfig(self):
		configdict={}
                config = '../config/Config.conf'
                with open(config) as config_file:
                        for lines in config_file:
                                if re.search(r'=',lines):
                                        key = lines.strip('\n').split('=')
                                        configdict[key[0]]=key[1]
                return configdict
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
				zattoo_list = self.ZattooList(final_vector,matrix,matrix1,collection2,"stot")
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
				zattoo_list = self.ZattooList(final_vector,matrix,matrix1,collection2,"stot")
				for values in zattoo_list:
					reclist.append(values)
			except:
				pass
		return reclist
	def CompareNewsToZattoo_sub(self,collection2,final_vector,lang1,language):
		reclist = []
		if (lang1=="de"):
			try:
				if (language=="de"):
					with open('./subtitlepicklefiles/docbivec_'+language+'_en.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
					matrix1 = np.genfromtxt('./subtitlepicklefiles/ids_de.copy',dtype='str')
				elif (language=="en"):
					with open('./subtitlepicklefiles/docbivec_'+language+'_de.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
                                        matrix1 = np.genfromtxt('./subtitlepicklefiles/ids_en.copy',dtype='str')
				zattoo_list = self.ZattooList(final_vector,matrix,matrix1,collection2,"sub")
				for values in zattoo_list:
					reclist.append(values)
			except:
				pass
		elif (lang1=="en"):
			try:
	                        if (language=="en"):
					with open('./subtitlepicklefiles/docbivec_'+language+'_de.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
                                        matrix1 = np.genfromtxt('./subtitlepicklefiles/ids_en.copy',dtype='str')
				elif (language=="de"):
					with open('./subtitlepicklefiles/docbivec_'+language+'_en.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
					matrix1 = np.genfromtxt('./subtitlepicklefiles/ids_de.copy',dtype='str')
				elif (language=="it"):
					with open('./subtitlepicklefiles/docbivec_'+language+'_en.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
                                        matrix1 = np.genfromtxt('./subtitlepicklefiles/ids_it.copy',dtype='str')
				elif (language=="es"):
					with open('./subtitlepicklefiles/docbivec_'+language+'_en.pkl', 'rb') as infile:
						matrix= pickle.load(infile)
                                    	matrix1 = np.genfromtxt('./subtitlepicklefiles/ids_es.copy',dtype='str')
				zattoo_list = self.ZattooList(final_vector,matrix,matrix1,collection2,"sub")
				for values in zattoo_list:
					reclist.append(values)
			except:
				pass
		return reclist
	def ZattooList(self,final_vector,matrix,matrix1,collection2,type1):
		appendlist = []
		a = np.array([final_vector])
                d = distance.cdist(matrix, a, 'correlation')
		sorted_list_ind = np.argsort(d, axis=0)
		sorted_list_val = np.sort(d, axis=0)
		for docs,val in zip(sorted_list_ind[0:5],sorted_list_val[0:5]):
			dic = collection2.find_one({"_id": ObjectId(matrix1[docs[0]])})
			zattoo_dict = {}
			tea = dic["SourceURL"].split("/")[-1].strip()
			zattoo_dict["cid"] = dic["CID"].strip()
			zattoo_dict["simscore"] = 1-val[0]
			start = 1000*int(arrow.get(dic["StartTime"].strip()).datetime.strftime("%s"))
			end = start+40000
			watch_url = "http://zattoo.com/watch/"+dic["CID"].strip()+"/"+str(tea)+"/"+str(start)+"/"+str(end)
			zattoo_dict["ZattooURL"] = watch_url
			zattoo_dict["Type"] = type1
			if bool(zattoo_dict):
				appendlist.append(zattoo_dict)
		return appendlist
	def pushrec(self,p,collection2,collection3,bulk):
			recommends_en=[]
			recommends_de=[]
			recommends_es=[]
			recommends_it=[]
			recommends_en_sub=[]
			recommends_de_sub=[]
			recommends_es_sub=[]
			recommends_it_sub=[]
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
				recommends_en_sub = self.CompareNewsToZattoo_sub(collection3,final_vec,p['Lang'],"en")
				recommends_de_sub = self.CompareNewsToZattoo_sub(collection3,final_vec,p['Lang'],"de")
			elif p['Lang']=="en":
				tokens = utils.simple_preprocess(p['Text'])
				s = shelve.open('./StoreWordVec/wiki/en_de_vec_wiki_shelf.db')
				final_vec = self.docvectors(s,tokens,doc_vector,final_vector)
				s.close()
				recommends_en = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"en")
				recommends_de = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"de")
				recommends_en_sub = self.CompareNewsToZattoo_sub(collection3,final_vec,p['Lang'],"en")
				recommends_de_sub = self.CompareNewsToZattoo_sub(collection3,final_vec,p['Lang'],"de")
				s = shelve.open('./StoreWordVec/wiki/en_es_vec_wiki_shelf.db')
				final_vec = self.docvectors(s,tokens,doc_vector,final_vector)
				s.close()
				recommends_es = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"es")
				recommends_es_sub = self.CompareNewsToZattoo(collection3,final_vec,p['Lang'],"es")
				s = shelve.open('./StoreWordVec/wiki/en_it_vec_wiki_shelf.db')
				final_vec = self.docvectors(s,tokens,doc_vector,final_vector)
				s.close()
				recommends_it = self.CompareNewsToZattoo(collection2,final_vec,p['Lang'],"it")
				recommends_it = self.CompareNewsToZattoo(collection3,final_vec,p['Lang'],"it")
			if len(recommends_en_sub)>0:
				dict_news_tv["tvrec_en"] = recommends_en+recommends_en_sub
				#dict_news_tv["tvrec_en"] = [j for i in zip(recommends_en,recommends_en_sub) for j in i]
				#dict_news_tv["tvrec_en"] = map(next, random.sample([iter(recommends_en)]*len(recommends_en) + [iter(recommends_en_sub)]*len(recommends_en_sub), len(recommends_en)+len(recommends_en_sub)))
			else:
				dict_news_tv["tvrec_en"] = recommends_en
			if len(recommends_de_sub)>0:
				dict_news_tv["tvrec_de"] = recommends_de+recommends_de_sub
				#dict_news_tv["tvrec_de"] = [j for i in zip(recommends_de,recommends_de_sub) for j in i]
				#dict_news_tv["tvrec_de"] = map(next, random.sample([iter(recommends_de)]*len(recommends_de) + [iter(recommends_de_sub)]*len(recommends_de_sub), len(recommends_de)+len(recommends_de_sub)))
			else:
				dict_news_tv["tvrec_de"] = recommends_de
			if len(recommends_es_sub)>0:
				dict_news_tv["tvrec_es"] = recommends_es+recommends_es_sub
				#dict_news_tv["tvrec_es"] = [j for i in zip(recommends_es,recommends_es_sub) for j in i]
				#dict_news_tv["tvrec_es"] = map(next, random.sample([iter(recommends_es)]*len(recommends_es) + [iter(recommends_es_sub)]*len(recommends_es_sub), len(recommends_es)+len(recommends_es_sub)))
			else:
				dict_news_tv["tvrec_es"] = recommends_es
			if len(recommends_it_sub)>0:
				dict_news_tv["tvrec_it"] = recommends_it+recommends_it_sub
				#dict_news_tv["tvrec_it"] = [j for i in zip(recommends_it,recommends_it_sub) for j in i]
				#dict_news_tv["tvrec_it"] = map(next, random.sample([iter(recommends_it)]*len(recommends_it) + [iter(recommends_it_sub)]*len(recommends_it_sub), len(recommends_it)+len(recommends_it_sub)))
			else:
				dict_news_tv["tvrec_it"] = recommends_it
				#collection5.remove(None, safe=True)
			try:
				bulk.insert(json.loads(json.dumps(dict_news_tv)),continue_on_error=True)
			except pymongo.errors.DuplicateKeyError:
				pass
	def calcsize(self):
		configdict=self.readConfig()
                if configdict['MongoDBPath']!="":
			client = MongoClient(configdict['MongoDBPath'])
			if configdict['MongoDBUserName']!="" and configdict['MongoDBPassword']!="":
                                client.the_database.authenticate(configdict['MongoDBUserName'],configdict['MongoDBPassword'],source=self.mdb)
				storedb = client[self.db]
				collection5 = storedb["tailablequeryzattoo"]
				collection2 = storedb["zattooasr"]
				collection3 = storedb["zattoosub"]
				bulk = storedb.returnzattoorec
				#collection5.insert({"Lang":"de", "Text":"test"}) test document
				cursor = collection5.find(tailable=True,await_data=True)
				while cursor.alive:
					try:
						doc = cursor.next()
						if doc!="":
							self.pushrec(doc,collection2,collection3,bulk)
					except StopIteration:
						time.sleep(0.1)
			else:
				print 'Please Set MongoDB UserName Password in Config file.'
		else:
			print 'Please Set MongoDB path in Config file.'
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
