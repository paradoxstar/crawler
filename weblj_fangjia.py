#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2016 Hongkun Yu <staryhk@gmail.com>
#
# @AUTHOR:      Hongkun Yu
# @MAIL:        staryhk@gmail.com
# @VERSION:     2017-1-9
#
import os
import re
import socket
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
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:49.0) Gecko/20100101 Firefox/49.0'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}\
    ]

 
storename = 'fangjia_' + time.strftime("%Y_%m_%d_%X", time.localtime())


class SQLiteWraper(object):
    
    def __init__(self,path,*args,**kwargs):  
        self.lock = threading.RLock()   
        self.path = path 
        
        self.conn = sqlite3.connect(self.path)
        self.conn.text_factory = str
        self.cu = self.conn.cursor()
      
    def close(self):  
        self.conn.close()  

    def execute(self,command):  
        cu = self.cu
        self.lock.acquire()
        try:
            if isinstance(command, (str, unicode)):
                cu.execute(command)
            elif len(command) == 2:
                cu.execute(command[0],command[1])
            else:
                raise ValueError("Invalid command")
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass
        finally:
            self.lock.release()



def gen_zufang_insert_command(info_dict):
    
    info_list=[u'搜索名称', u'小区名称', u'小区均价', u'在售房源', u'90天成交', u'平均成交周期', u'30天成交',u'30天看房']

    t = []
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t = tuple(t)
    command=(r"insert into fangjia values(?,?,?,?,?,?,?,?)",t)
    return command

    
def xiaoqu_fangjia_spider(db_fj, xq_name = u"京师园"):
  
    trytimes = 0
    url=u"http://bj.lianjia.com/fangjia/rs"+urllib2.quote(xq_name)+"/"
    while 1:
        try:
            req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
            source_code = urllib2.urlopen(req,timeout=10).read()
            plain_text=unicode(source_code)#,errors='ignore')   
            soup = BeautifulSoup(plain_text)
        except socket.timeout as e:
            if trytimes < 5:
                time.sleep(2)
                trytimes += 1
                continue
            else:
                print e
                exception_write(e, 'xiaoqu_fangjia_spider', xq_name)
                return
        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            exception_write(e, 'xiaoqu_fangjia_spider',xq_name)
            return
        except Exception,e:
            print e
            exception_write(e, 'xiaoqu_fangjia_spider',xq_name)
            return
    
        human = soup.find('div',{'class':'human'})
        if not human:
            break
        else:
            print "block! && wait"
            time.sleep(random.randint(900, 1200))
            trytimes = 0
    

    record = soup.find('div',{'class':'m-tongji'})
    if not record:
        print "no fangjia tongji record"
        return

    shuju = record.find('div', {'class': 'shuju'})
    title = ""
    if shuju:
        title = shuju.find('div', {'class': 'tit'}).text

    qushi_2 = record.find('div', {'class': 'qushi-2'})

    avP = ""
    onsa = ""
    trade_90 = ""

    if qushi_2:
        aP = qushi_2.find('span', {'class': 'num'})
        onsAndTrade = qushi_2.findAll('a')
    else:
        print "no qushi_2"

    if aP:
        avP = aP.text
    if len(onsAndTrade) == 2:
        onsa = onsAndTrade[0].text
        trade_90 = onsAndTrade[1].text


    aC = ""
    trade_30 = ""
    rS = ""
    item = record.findAll('div', {'class': 'item item-1-3'})

    if len(item) == 3:
        aveC = item[0].find('div', {'class': 'num'})
        t_30 = item[1].find('div', {'class': 'num'})
        rShow= item[2].find('div', {'class': 'num'})
    else:
        print "no item info"
        return

    if aveC:
        aC = aveC.text
    if t_30:
        trade_30 = t_30.text
    if rShow:
        rS = rShow.text

    info_dict={}
    info_dict[u'搜索名称'] = xq_name
    info_dict[u'小区名称'] = title
    info_dict[u'小区均价'] = avP
    info_dict[u'在售房源'] = onsa
    info_dict[u'90天成交'] = trade_90
    info_dict[u'平均成交周期'] = aC
    info_dict[u'30天成交'] = trade_30
    info_dict[u'30天看房'] = rS
   
    #print info_dict
    command = gen_zufang_insert_command(info_dict)
    #print command
    try:
        db_fj.execute(command)
    except Exception as e:
        print e
        exception_write(e, 'xiaoqu_fangjia_spider',xq_name)

    print xq_name + "has done!"


def exception_write(e, fun_name,url):
    f = open(storename + '_log.txt','a')
    line="%s\t%s\t%s\n" % (e, fun_name,url)
    f.write(line)
    f.close()

def excepthandle(db_fj):
    pass

if __name__=="__main__":
  
    db_fj = SQLiteWraper('fangjia.db')
    
    create_command = """create table if not exists fangjia
                (href TEXT primary key UNIQUE,
                name TEXT, 
                averPrice TEXT,
                onsale TEXT,
                recentTradeNinty TEXT,
                averCycle TEXT,
                recentTradeThirty TEXT,
                recentShow TEXT)"""
   
    db_fj.execute(create_command)
    
    xq_list=[]
    xq = open("xiaoqu_2016_11_06_23_01_24_xiaoqu_district_list.txt", "r")
    for line in xq:
        xq_list.append(line.strip('\n'))
    xq.close()
    total = len(xq_list)
    
    #print xq_list[0]
    print "total number of xiaoqu is %d" % total
    
    #excepthandle(db_zf)
    #print 'Exception handle done'
    
    for xq in xq_list:
        print 'begin spidering xiaoqu %s' % xq
        xiaoqu_fangjia_spider(db_fj,xq)
        #time.sleep(random.randint(3, 5))

    db_fj.close()

    print 'all done'

    
