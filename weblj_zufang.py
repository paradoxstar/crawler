#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2016 Hongkun Yu <staryhk@gmail.com>
#
# @AUTHOR:      Hongkun Yu
# @MAIL:        staryhk@gmail.com
# @VERSION:     2016-8-25
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

 
storename = 'zufang_' + time.strftime("%Y_%m_%d_%X", time.localtime())


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
        finally:
            self.lock.release()



def gen_zufang_insert_command(info_dict):
    
    info_list=[u'成交网页',u'小区名称',u'户型',u'租住面积',u'朝向',u'楼层',u'小区ID',u'签约日期',u'总层数',u'成交价',u'装修情况',u'数据来源']

    t = []
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t = tuple(t)
    command=(r"insert into zufang values(?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command

    
def xiaoqu_zufang_spider(db_zf, xq_name = u"京师园"):
  
    trytimes = 0
    url=u"http://bj.lianjia.com/zufang/rs"+urllib2.quote(xq_name)+"/"
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
                exception_write(e, 'xiaoqu_zufang_spider', xq_name)
                return
        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            exception_write(e, 'xiaoqu_zufang_spider',xq_name)
            return
        except Exception,e:
            print e
            exception_write(e, 'xiaoqu_zufang_spider',xq_name)
            return
    
        human = soup.find('div',{'class':'human'})
        if not human:
            break
        else:
            print "block! && wait"
            time.sleep(600)
            trytimes = 0
    

    record = soup.find('li',{'data-index':'0'})        
    if not record:
        print "no zufang record"
        return

    records = soup.find('ul', {'id': 'house-lst'}).findAll('li')
    found = 0
    for reco in records:
        ack = reco.find('div', {'class': 'where'}).a.text.strip()
        if unicode(xq_name) == unicode(ack):
            found = 1
            break

    if found == 0:
        print "no zufang record actually"
        return

    recordurl = unicode(reco.a['href'])
    zufang_item_page(db_zf, recordurl)
    
    print xq_name + "has done!"


def zufang_item_page(db_zf, url):
    
    trytimes = 0
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
                exception_write(e, 'zufang_item_page', url)
                return
        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            exception_write(e, 'zufang_item_page', url)
            return
        except Exception,e:
            print e
            exception_write(e, 'zufang_item_page', url)
            return

        human = soup.find('div',{'class':'human'})
        if not human:
            break
        else:
            print "block! && wait"
            time.sleep(600)
            trytimes = 0
    
    zufang_list = soup.find('div',{'class':'dealRecord'})
    if not zufang_list:
        print "page has no zufang_list"
        return
    
    houseid = url[29:-5] 
    xiaoquid = unicode(soup.find('div',{'class':'zf-room'}).findAll('p')[5].a['href'][8:-1])
    #houseid = soup.find('div',{'class':'houseRecord'}).span.text[5:]
   
    get_zufang_xiaoqu_data(db_zf, houseid, xiaoquid)


def get_zufang_xiaoqu_data(db_zf, houseid, xiaoquid):
    
    zfurl = "http://bj.lianjia.com/zufang/housestat?hid=%s&rid=%s" %(houseid, xiaoquid)
    trytimes = 0
    while 1:
        try:
            req = urllib2.Request(zfurl,headers=hds[random.randint(0,len(hds)-1)])
            source_code = urllib2.urlopen(req,timeout=10).read()
            alldata = json.loads(source_code)
        except socket.timeout as e:
            if trytimes < 5:
                time.sleep(2)
                trytimes += 1
                continue
            else:
                print e
                exception_write(e, 'get_zufang_xiaoqu_data', zfurl)
                return
        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            exception_write(e, 'get_zufang_xiaoqu_data', zfurl)
            return
        except Exception,e:
            print e
            exception_write(e, 'get_zufang_xiaoqu_data', zfurl)
            return

        if alldata.has_key('data'):
            break
        else:
            print "block! && wait"
            time.sleep(600)
            trytimes = 0

    if not alldata['data'].has_key('resblockSold'):
        print "no zufang data"
        return

    numdata = len(alldata['data']['resblockSold'])
    for i in range(numdata):
        info_dict={}
        zfrecord = alldata['data']['resblockSold'][i]
        info_dict[u'小区名称'] = zfrecord['resblockName']
        info_dict[u'小区ID'] = xiaoquid
        info_dict[u'成交网页'] = zfrecord['viewUrl']
        info_dict[u'签约日期'] = zfrecord['transDate']
        info_dict[u'总层数'] = zfrecord['totalFloor']
        info_dict[u'户型'] = zfrecord['title']
        info_dict[u'数据来源'] = zfrecord['signSource']
        info_dict[u'成交价'] = zfrecord['price']
        info_dict[u'朝向'] = zfrecord['orientation']
        info_dict[u'楼层'] = zfrecord['floor']
        info_dict[u'租住面积'] = zfrecord['area']
        info_dict[u'装修情况'] = zfrecord['decoration']
        #print info_dict
        command = gen_zufang_insert_command(info_dict)
        #print command
        try:
            db_zf.execute(command)
        except Exception as e:
            print e
            exception_write(e, "get_zufang_xiaoqu_data", zfurl + "\t" + str(i))


def exception_write(e, fun_name,url):
    f = open(storename + '_log.txt','a')
    line="%s\t%s\t%s\n" % (e, fun_name,url)
    f.write(line)
    f.close()

def excepthandle(db_cj):
    xzs = [
'尚西泊图',
'展春园',
'山水倾城',
'山水巢东',
'山水文园一期',
'山水文园二期',
'山水文园五期',
'山水文园四期',
'常秀家园',
'常青园一区',
'常青园北里',
'常青藤嘉园',
'帽儿胡同45号院',
'干杨树',
'干面胡同',
'平乐园小区',
'平原里小区',
'平安嘉苑',
'幸福二村',
'幸福一村西里',
'幸福东区',
'幸福北里',
'幸福南里',
'幸福家园1号院',
'幸福家园5号院',
'幸福家园一期',
'幸福家园二期',
'幸福时光',
'幸福艺居',
'幸福西区',
'幸福路6号院',
'幻星家园',
'广益大厦',
'广义街10号院',
'广义里小区',
'广信嘉园',
'广华轩',
'广厦鑫苑',
'广和东里',
'广和里',
'广外南街50号院',
'广外南街63号院',
'广宁村',
'广安小区',
'广安苑一期',
'广安路54号院',
'广安门内大街',
'广安门北街',
'广安门南街',
'广安门外南街',
'广安门外南街67号院',
'广安门外大街',
'广安门外车站东街',
'广安门小区',
'广安﹒康馨家园',
'广泰小区',
'广渠家园',
'广渠路',
'广渠门内大街',
'广渠门外南街'
]
    zuip = [
u'http://bj.lianjia.com/zufang/101100735278.html',
u'http://bj.lianjia.com/zufang/101100776071.html',
u'http://bj.lianjia.com/zufang/101100767239.html'
]

    for xq in xzs:
        print 'begin spidering xiaoqu %s' % xq
        xiaoqu_zufang_spider(db_cj, xq)

    for cp in zuip:
        print 'spider %s' % cp
        zufang_item_page(db_cj, cp)



if __name__=="__main__":
  
    db_zf = SQLiteWraper('zufang.db')
    
    create_command = """create table if not exists zufang 
                (href TEXT primary key UNIQUE,
                name TEXT, 
                style TEXT, 
                area TEXT, 
                orientation TEXT, 
                floor TEXT, 
                id TEXT, 
                sign_time TEXT, 
                totalfloor TEXT, 
                price TEXT, 
                decoration TEXT, 
                signSource TEXT)"""
   
    db_zf.execute(create_command)
    
    xq_list=[]
    xq = open("zufang_8_31_out_11_4_in.txt", "r")
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
        xiaoqu_zufang_spider(db_zf,xq)
        #time.sleep(random.randint(3, 5))

    db_zf.close()

    print 'all done'

    
