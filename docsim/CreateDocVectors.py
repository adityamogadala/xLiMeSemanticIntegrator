#-*- coding: utf-8 -*-
#==============================================================================
#Description     : Create vectors for SpeechtoText, Subtitles etc..
#Author          : Aditya Mogadala 
#email           : aditya.mogadala@kit.edu
#Version         : 1.0.1
#Copyright       : Institute AIFB, Karlsruhe Institute of Technology (KIT)
#==============================================================================

import TVStoTDocVectors
import TVSubDocVectors
import re
import os
def readconfig():
	configdict={}
        config = '../config/Config.conf'
        with open(config) as config_file:
                for lines in config_file:
                        if re.search(r'=',lines):
                                key = lines.strip('\n').strip().split('=')
                                configdict[key[0]]=key[1]
	return configdict
def main():
	configdic = readconfig()
	template="docvecpkl"
	#### SpeechtoText Doc Vectors ####
	directory="./stot"+template
	if not os.path.exists(directory):
	    os.makedirs(directory)
	speechtotext = TVStoTDocVectors.SpeechtoTextVectors(configdic)
	speechtotext.computevectors()
	#### Subtitle Doc Vectors #####
	directory="./subtitle"+template
	if not os.path.exists(directory):
	    os.makedirs(directory)
	subtitles = TVSubDocVectors.SubtitlesVectors(configdic)
	subtitles.computevectors()
	##### Add more for Social Media, News etc #######
if  __name__ =='__main__':
        main()
