#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2016 Hongkun Yu <staryhk@gmail.com>
#
# @AUTHOR:      Hongkun Yu
# @MAIL:        staryhk@gmail.com
# @VERSION:     2016-10-24
#

import os
import re
import urllib2  
import sqlite3
import random
import threading
from bs4 import BeautifulSoup
import json
import time

import sys
reload(sys)
sys.setdefaultencoding("utf-8")


#Some User Agents
hds=[{'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'},\
   # {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
   # {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
   # {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
   # {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
   # {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
   # {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
   # {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
   # {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
   # {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
   # {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
   # {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
   # {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
   # {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}\
    ]

    
def network_search(lianjianet):
    
    try:
        req = urllib2.Request(lianjianet,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=20).read()
        plain_text=source_code.decode("gb2312",errors='ignore')   
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        return
    except Exception,e:
        print e
        return
    
    networks = soup.find('ul', {'class':'LifeList clearfix'}).findAll('a')

    print len(networks)
    for i in range(len(networks)):
        name = networks[i].text 
        staturl = networks[i]['href']
        
        print str(i) + ":" + name 

        try:
            req = urllib2.Request(staturl,headers=hds[random.randint(0,len(hds)-1)])
            source_code = urllib2.urlopen(req,timeout=20).read()
            plain_text=source_code.decode("gb2312",errors='ignore')   
            soup = BeautifulSoup(plain_text)
        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            return
        except Exception,e:
            print e
            return

        
        tables = soup.find('table', {'id':'content'}).findAll('tr')
        info_dict={}
        for k in range(len(tables)):
            kvp = tables[k].findAll('td')
            info_dict.update({kvp[0].text: kvp[1].text})


        for key in info_dict:
            print key + " " + info_dict[key]

        print ""





if __name__=="__main__":
    
    net = u"http://www.iecity.com/beijing/brand/108.html"
    net2 = u"http://www.iecity.com/beijing/brand/108_2.html"
    
    
    #network_search(net)
    network_search(net2)


    print 'done'

    
