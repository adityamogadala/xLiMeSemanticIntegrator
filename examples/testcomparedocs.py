# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '../docsim')
import CompareTwoDocs

text1 = "I do not speak english"
text2 = "München bietet in seinen diversen Abteilungen immer"
#text2 = "A statement from Mr Obama's press secretary read: The President will make an historic visit to Hiroshima with Prime Minister [Shinzo] Abe to highlight his continued commitment to pursuing peace and security in a world without nuclear weapons."
comparedoc = CompareTwoDocs.CompareDocContent()
score = comparedoc.compare(text1,text2)
print 'Score between 0 and 1: ', score

