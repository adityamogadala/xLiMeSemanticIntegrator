# -*- coding: utf-8 -*-
#==============================================================================
#Description     : Compare Two Documents or Snippets using Word Vector Representations (Monolingual and Cross-Lingual).
#Author          : Aditya Mogadala 
#email           : aditya.mogadala@kit.edu
#Version         : 1.0.1
#Copyright       : Institute AIFB, Karlsruhe Institute of Technology (KIT)
#==============================================================================
from scipy.spatial import distance
from gensim import utils
import shelve
import numpy as np
import sys
import langid

class CompareDocContent:
	def __init__(self):
		self.wordvecdir='./StoreWordVec/wiki/'
	def detectlang(self,text):
		langid.set_languages(['en','de','es','it'])
		return langid.classify(text)[0]
	def getvector(self,text,shelvedb):
		doc_vector=np.array([0.0]*100)
                final_vector=np.array([0.0]*100)
		tokens = utils.simple_preprocess(text)
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
	def monolingualcomp(self,lang1,text1,text2):
		s = shelve.open(self.wordvecdir+'en_de_vec_wiki_shelf.db')
		text1_vec = self.getvector(text1,s)
		text2_vec = self.getvector(text2,s)
		s.close()
		d = distance.cdist(np.array([text1_vec]),np.array([text2_vec]), 'cosine')
		return 1.0-d[0][0]
	def crosslingualcomp(self,lang1,text1,lang2,text2):
		s_lang1 = shelve.open(self.wordvecdir+'en_de_vec_wiki_shelf.db')
		text1_vec = self.getvector(text1,s_lang1)
		s_lang1.close()
		s_lang2 = shelve.open(self.wordvecdir+'de_en_vec_wiki_shelf.db')
		text2_vec = self.getvector(text2,s_lang2)
		s_lang2.close()
		d = distance.cdist(np.array([text1_vec]),np.array([text2_vec]), 'cosine')
		return 1.0-d[0][0]
	def compare(self,text1,text2):
		vals = float('inf')
		lang1 = self.detectlang(text1)
		lang2 = self.detectlang(text2)
		if lang1 and lang2 in ['en','de', 'it' ,'es']:
			print 'First Document Language: ', lang1
			print 'Second Document Language: ', lang2
			if lang1==lang2:
				print 'Monolingual Document Comparison..... '
				vals = self.monolingualcomp(lang1,text1,text2)
			else:
				print 'Cross-Lingual Document Comparison..... '
				vals = self.crosslingualcomp(lang1,text1,lang2,text2)
		else:
			print 'currently we dont support this language. Only between EN and Other EU languages'
		return vals
